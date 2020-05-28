package ai.tripl.arc.transform

import java.net.URI
import java.nio.ByteBuffer
import java.security.SecureRandom
import java.sql.Date
import java.sql.Timestamp
import java.time.temporal.ChronoUnit
import java.time.temporal.TemporalAdjusters._
import java.time.ZoneId
import java.util.ServiceLoader
import org.bouncycastle.crypto.generators._
import org.bouncycastle.crypto.params._

import scala.collection.JavaConverters._
import scala.util.{Try, Success, Failure}
import scala.util.Properties._

import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.{StringEntity, ByteArrayEntity}
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager
import org.apache.http.impl.client.HttpClients
import org.apache.http.impl.client.LaxRedirectStrategy

import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._

import scala.io.Source

import ai.tripl.arc.api.API._
import ai.tripl.arc.util._

import ai.tripl.arc.api.API._
import ai.tripl.arc.config._
import ai.tripl.arc.config.Error._
import ai.tripl.arc.plugins.PipelineStagePlugin
import ai.tripl.arc.util.DetailException
import ai.tripl.arc.util.EitherUtils._
import ai.tripl.arc.util.HTTPUtils
import ai.tripl.arc.util.Utils
import ai.tripl.arc.util.Mask
import ai.tripl.arc.transform.codec.Argon2Codec

class MaskDataTransform extends PipelineStagePlugin {

  val version = ai.tripl.arc.maskdata.BuildInfo.version

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val signature = s"MaskDataTransform environment variable '${MaskDataTransform.passphraseEnvironmentVariable}' must be a string of between 64 and 256 characters."

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "outputView" :: "persist" :: "params" :: Nil
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val inputView = getValue[String]("inputView")
    val outputView = getValue[String]("outputView")
    val persist = getValue[java.lang.Boolean]("persist", default = Some(false))
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    // load codecs. these are not serializable so have to be re-initalised on each executor but are useful for logging
    val serviceLoader = Utils.getContextOrSparkClassLoader
    val maskDataTransformCodecs = ServiceLoader.load(classOf[MaskDataTransformCodec], serviceLoader).iterator().asScala.toList.map { codec => s"${codec.getClass.getName}:${codec.getVersion}" }
    val codecs = if (maskDataTransformCodecs.length != 0) Right(maskDataTransformCodecs) else Left(ConfigError("MaskDataTransformCodec", None, "No codecs found to perform data masking.") :: Nil)

    // get the passphrase from an environment variable
    val passphrase = envOrNone(MaskDataTransform.passphraseEnvironmentVariable) match {
      case Some(value) if (value.length < 64) || (value.length > 256) => Left(ConfigError(MaskDataTransform.passphraseEnvironmentVariable, None, signature) :: Nil)
      case Some(value) => Right(Option(value))
      case None => Right(None)
    }

    (name, description, inputView, outputView, persist, passphrase, codecs, invalidKeys) match {
      case (Right(name), Right(description), Right(inputView), Right(outputView), Right(persist), Right(passphrase), Right(codecs), Right(invalidKeys)) =>

        val stage = MaskDataTransformStage(
          plugin=this,
          name=name,
          description=description,
          inputView=inputView,
          outputView=outputView,
          persist=persist,
          params=params,
          passphrase=passphrase,
        )

        stage.stageDetail.put("inputView", inputView)
        stage.stageDetail.put("outputView", outputView)
        stage.stageDetail.put("persist", java.lang.Boolean.valueOf(persist))
        stage.stageDetail.put("codecs", maskDataTransformCodecs.asJava)

        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, description, inputView, outputView, persist, passphrase, codecs, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
}

object MaskDataTransform {
  val passphraseEnvironmentVariable = "ETL_CONF_MASK_DATA_PASSPHRASE"
}

case class MaskDataTransformStage(
    plugin: MaskDataTransform,
    name: String,
    description: Option[String],
    inputView: String,
    outputView: String,
    persist: Boolean,
    passphrase: Option[String],
    params: Map[String, String]
  ) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    MaskDataTransformStage.execute(this)
  }
}

sealed trait Treatment {}
sealed trait CodecTreatment extends Treatment {
  def codec: MaskDataTransformCodec
}
object Treatment {
  val MASK_KEY = "mask"
  val TREATMENT_KEY = "treatment"

  // date treatments
  case class RandomDate(deterministic: Boolean, threshold: Long, codec: MaskDataTransformCodec) extends CodecTreatment
  case class MonthDateTruncate() extends Treatment
  case class YearDateTruncate() extends Treatment

  // string treatments
  case class RandomString(deterministic: Boolean, length: Int, alphabet: String, codec: MaskDataTransformCodec) extends CodecTreatment
  case class RandomList(deterministic: Boolean, locale: String, list: String, codec: MaskDataTransformCodec) extends CodecTreatment
  case class RandomEmail(deterministic: Boolean, locale: String, codec: MaskDataTransformCodec) extends CodecTreatment

  def fromMetadata(maskDataTransformCodecs: Map[String, MaskDataTransformCodec], field: StructField): Option[Treatment] = {
    val metadata = field.metadata

    if (metadata.contains(MASK_KEY)) {
        Try(metadata.getMetadata(MASK_KEY)) match {
          case Success(mask) => {
            Try(mask.getString(TREATMENT_KEY)) match {

              // add additional strategies here
              case Success("randomDate") => {
                if (field.dataType != DateType && field.dataType != TimestampType) {
                  throw new Exception(s"Data Masking treatment 'randomDate' can only be applied to 'date' or 'timestamp' type but field '${field.name}' is of type '${field.dataType.simpleString}'.")
                }
                val algorithm = Try(mask.getString("algorithm").trim) match {
                  case Success(algo) => maskDataTransformCodecs.getOrElse(algo, throw new Exception(s"No codec named '${algo}'. Available codecs: ${maskDataTransformCodecs.keys.mkString("[", ", ", "]")}"))
                  case _ => new Argon2Codec()
                }
                val deterministic = Try(mask.getBoolean("deterministic")) match {
                  case Success(value) => value
                  case Failure(_) => false
                }
                val threshold = Try(mask.getLong("threshold")) match {
                  case Success(value) if (value == 0) => throw new Exception("Data Masking treatment 'randomDate' cannot be performed with 'threshold' set to 0.")
                  case Success(value) => value
                  case Failure(_) => 365L
                }
                Option(
                  Treatment.RandomDate(
                    deterministic,
                    threshold,
                    algorithm
                  )
                )
              }
              case Success("monthDateTruncate") => {
                if (field.dataType != DateType && field.dataType != TimestampType) {
                  throw new Exception(s"Data Masking treatment 'monthDateTruncate' can only be applied to 'date' or 'timestamp' type but field '${field.name}' is of type '${field.dataType.simpleString}'.")
                }
                Option(
                  Treatment.MonthDateTruncate()
                )
              }
              case Success("yearDateTruncate") => {
                if (field.dataType != DateType && field.dataType != TimestampType) {
                  throw new Exception(s"Data Masking treatment 'yearDateTruncate' can only be applied to 'date' or 'timestamp' type but field '${field.name}' is of type '${field.dataType.simpleString}'.")
                }
                Option(
                  Treatment.YearDateTruncate()
                )
              }

              case Success("randomString") => {
                if (field.dataType != StringType) {
                  throw new Exception(s"Data Masking treatment 'randomString' can only be applied to 'string' type but field '${field.name}' is of type '${field.dataType.simpleString}'.")
                }
                val algorithm = Try(mask.getString("algorithm").trim) match {
                  case Success(algo) => maskDataTransformCodecs.getOrElse(algo, throw new Exception(s"No codec named '${algo}'. Available codecs: ${maskDataTransformCodecs.keys.mkString("[", ", ", "]")}"))
                  case _ => new Argon2Codec()
                }
                val deterministic = Try(mask.getBoolean("deterministic")) match {
                  case Success(value) => value
                  case Failure(_) => false
                }
                val length = Try(mask.getLong("length")) match {
                  case Success(value) => value.toInt
                  case Failure(_) => 32
                }
                val alphabet = Try(mask.getString("alphabet")) match {
                  case Success(value) => value
                  case Failure(_) => "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ"
                }
                Option(
                  Treatment.RandomString(
                    deterministic,
                    length,
                    alphabet,
                    algorithm
                  )
                )
              }
              case Success("randomNumberString") => {
                if (field.dataType != StringType) {
                  throw new Exception(s"Data Masking treatment 'randomNumberString' can only be applied to 'string' type but field '${field.name}' is of type '${field.dataType.simpleString}'.")
                }
                val algorithm = Try(mask.getString("algorithm").trim) match {
                  case Success(algo) => maskDataTransformCodecs.getOrElse(algo, throw new Exception(s"No codec named '${algo}'. Available codecs: ${maskDataTransformCodecs.keys.mkString("[", ", ", "]")}"))
                  case _ => new Argon2Codec()
                }
                val deterministic = Try(mask.getBoolean("deterministic")) match {
                  case Success(value) => value
                  case Failure(_) => false
                }
                val length = Try(mask.getLong("length")) match {
                  case Success(value) => value.toInt
                  case Failure(_) => 32
                }
                Option(
                  Treatment.RandomString(
                    deterministic,
                    length,
                    "0123456789",
                    algorithm
                  )
                )
              }
              case Success("randomList") => {
                if (field.dataType != StringType) {
                  throw new Exception(s"Data Masking treatment 'randomList' can only be applied to 'string' type but field '${field.name}' is of type '${field.dataType.simpleString}'.")
                }
                val algorithm = Try(mask.getString("algorithm").trim) match {
                  case Success(algo) => maskDataTransformCodecs.getOrElse(algo, throw new Exception(s"No codec named '${algo}'. Available codecs: ${maskDataTransformCodecs.keys.mkString("[", ", ", "]")}"))
                  case _ => new Argon2Codec()
                }
                val deterministic = Try(mask.getBoolean("deterministic")) match {
                  case Success(value) => value
                  case Failure(_) => false
                }
                val locale = Try(mask.getString("locale")) match {
                  case Success(value) => {
                    val keySet = Mask.localizedLists.keySet
                    if (keySet.contains(value)) {
                      value
                    } else {
                      val possibleKeys = ConfigUtils.levenshteinDistance(keySet.toSeq, value)(4)
                      if (possibleKeys.isEmpty) {
                        throw new Exception(s"""Data Masking treatment 'randomList' has invalid 'locale': '${value}'.""")
                      } else {
                        throw new Exception(s"""Data Masking treatment 'randomList' has invalid 'locale': '${value}'. Perhaps you meant one of: ${possibleKeys.map(field => s"'${field}'").mkString("[",", ","]")}.""")
                      }
                    }
                  }
                  case Failure(_) => throw new Exception(s"Data Masking treatment 'randomList' requires 'locale' attribute.")
                }
                val list = Try(mask.getString("list")) match {
                  case Success(value) => {
                    val keySet = Mask.localizedLists.get(locale).get.keySet
                    if (keySet.contains(value)) {
                      value
                    } else {
                      val possibleKeys = ConfigUtils.levenshteinDistance(keySet.toSeq, value)(4)
                      if (possibleKeys.isEmpty) {
                        throw new Exception(s"""Data Masking treatment 'randomList' has invalid 'list': '${value}'.""")
                      } else {
                        throw new Exception(s"""Data Masking treatment 'randomList' has invalid 'list': '${value}'. Perhaps you meant one of: ${possibleKeys.map(field => s"'${field}'").mkString("[",", ","]")}.""")
                      }
                    }
                  }
                  case Failure(_) => throw new Exception(s"Data Masking treatment 'randomList' requires 'list' attribute.")
                }
                Option(
                  Treatment.RandomList(
                    deterministic,
                    locale,
                    list,
                    algorithm
                  )
                )
              }
              case Success("randomEmail") => {
                if (field.dataType != StringType) {
                  throw new Exception(s"Data Masking treatment 'randomEmail' can only be applied to 'string' type but field '${field.name}' is of type '${field.dataType.simpleString}'.")
                }
                val algorithm = Try(mask.getString("algorithm").trim) match {
                  case Success(algo) => maskDataTransformCodecs.getOrElse(algo, throw new Exception(s"No codec named '${algo}'. Available codecs: ${maskDataTransformCodecs.keys.mkString("[", ", ", "]")}"))
                  case _ => new Argon2Codec
                }
                val deterministic = Try(mask.getBoolean("deterministic")) match {
                  case Success(value) => value
                  case Failure(_) => false
                }
                val locale = Try(mask.getString("locale")) match {
                  case Success(value) => {
                    val keySet = Mask.localizedLists.keySet
                    if (keySet.contains(value)) {
                      value
                    } else {
                      val possibleKeys = ConfigUtils.levenshteinDistance(keySet.toSeq, value)(4)
                      if (possibleKeys.isEmpty) {
                        throw new Exception(s"""Data Masking treatment 'randomEmail' has invalid 'locale': '${value}'.""")
                      } else {
                        throw new Exception(s"""Data Masking treatment 'randomEmail' has invalid 'locale': '${value}'. Perhaps you meant one of: ${possibleKeys.map(field => s"'${field}'").mkString("[",", ","]")}.""")
                      }
                    }
                  }
                  case Failure(_) => throw new Exception(s"Data Masking treatment 'randomEmail' requires 'locale' attribute.")
                }
                Option(
                  Treatment.RandomEmail(
                    deterministic,
                    locale,
                    algorithm
                  )
                )
              }
              case _ => None
            }
          }
          case Failure(_) => None
        }
    } else {
      None
    }
  }
}

trait MaskDataTransformCodec {
  def sparkName(): String
  def sparkString(): String
  def getVersion(): String

  def encrypt(value: Array[Char], salt: Array[Byte]): Array[Byte]

  def hash(value: String, deterministic: Boolean, passphrase: Option[String]): Array[Byte] = {
    if (deterministic) {

      val passphraseBytes = passphrase.getOrElse(throw new Exception(MaskDataTransformCodec.signature)).getBytes

      val (pass, salt) = passphraseBytes.splitAt((passphraseBytes.length / 2))
      encrypt(
        value.toCharArray ++ pass.map(_.toChar),
        salt
      )
    } else {
      val secureRandom = new SecureRandom()
      val randomBytes = new Array[Byte](MaskDataTransformCodec.DEFAULT_HASH_LENGTH)
      secureRandom.nextBytes(randomBytes)
      randomBytes
    }
  }

  // ByteBuffer.wrap(bytes).getLong returns positive and negative values
  def hashLong(value: String, deterministic: Boolean, passphrase: Option[String]): Long = {
    ByteBuffer.wrap(hash(value, deterministic, passphrase)).getLong
  }
}

object MaskDataTransformCodec {
  val signature = s"MaskDataTransform environment variable '${MaskDataTransform.passphraseEnvironmentVariable}' is required for deterministic data masking."

  val DEFAULT_HASH_LENGTH = 64
}

object MaskDataTransformStage {

  /** Phantom Type to enable compiler to find the encoder we want
    */
  type TransformedRow = Row

  def execute(stage: MaskDataTransformStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    import spark.implicits._

    val df = spark.table(stage.inputView)
    val passphrase = stage.passphrase

    // set encoder
    implicit val encoder: Encoder[TransformedRow] = org.apache.spark.sql.catalyst.encoders.RowEncoder(df.schema)

    // initialise statistics accumulators or reset if they exist
    val maskedAccumulator = spark.sparkContext.longAccumulator
    val nonMaskedAccumulator = spark.sparkContext.longAccumulator

    var transformedDF = try {
      df.mapPartitions[TransformedRow] { partition: Iterator[Row] =>

        // get type and index so it doesnt have to be resolved for each row
        val bufferedPartition = partition.buffered

        bufferedPartition.hasNext match {
          case false => bufferedPartition
          case true => {

            // get the schema
            val schema = bufferedPartition.head.schema

            // load codecs. these are not serializable so have to be re-initalised on each executor
            val serviceLoader = Utils.getContextOrSparkClassLoader
            val maskDataTransformCodecs = ServiceLoader.load(classOf[MaskDataTransformCodec], serviceLoader).iterator().asScala.toList.map { codec => (codec.sparkName, codec) }.toMap

            // create an array of tuple(DataType, Option[Treatement to apply], field index to apply to)
            val treatments = schema.fields
              .zipWithIndex
              .map { case (field, index) => (field.dataType, Treatment.fromMetadata(maskDataTransformCodecs, field), index) }

            def encodeAsString(bytes: Array[Byte], alphabet: String) = bytes.map { byte => alphabet(Math.abs(byte.toInt) % alphabet.length) }.mkString
            def truncateString(value: String, length: Int, algorithm: String): String = {
              if (value.length < length) {
                throw new Exception(s"'${algorithm}' cannot produce required ${length} length output based on the input")
              }
              value.substring(0, length)
            }
            val utcZoneId = ZoneId.of("UTC")

            bufferedPartition.map { row =>
              val masked = treatments.foldLeft[Seq[Any]](row.toSeq) { case (state, (dataType, treatment, index)) =>
                (treatment, dataType) match {

                  case (None, _) =>
                    nonMaskedAccumulator.add(1)
                    state

                  case (Some(_), _) if row.isNullAt(index) =>
                    nonMaskedAccumulator.add(1)
                    state

                  case (Some(_: Treatment.MonthDateTruncate), DateType) =>
                    maskedAccumulator.add(1)
                    state.updated(index, Date.valueOf(row.getDate(index).toLocalDate.withDayOfMonth(1)))

                  case (Some(_: Treatment.MonthDateTruncate), TimestampType) =>
                    maskedAccumulator.add(1)
                    val timestamp = row.getTimestamp(index)
                    state.updated(index, Timestamp.from(timestamp.toInstant.atZone(utcZoneId).withDayOfMonth(1).toInstant))

                  case (Some(_: Treatment.YearDateTruncate), DateType) =>
                    maskedAccumulator.add(1)
                    state.updated(index, Date.valueOf(row.getDate(index).toLocalDate.withDayOfMonth(1).withMonth(1)))

                  case (Some(_: Treatment.YearDateTruncate), TimestampType) =>
                    maskedAccumulator.add(1)
                    val timestamp = row.getTimestamp(index)
                    state.updated(index, Timestamp.from(timestamp.toInstant.atZone(utcZoneId).withDayOfMonth(1).withMonth(1).toInstant))

                  case (Some(t: Treatment.RandomDate), DateType) =>
                    maskedAccumulator.add(1)
                    val date = row.getDate(index)
                    val numDays = t.codec.hashLong(date.toString, t.deterministic, passphrase) % t.threshold
                    state.updated(index, Date.valueOf(date.toLocalDate.plusDays(numDays)))

                  case (Some(t: Treatment.RandomDate), TimestampType) =>
                    maskedAccumulator.add(1)
                    val timestamp = row.getTimestamp(index)
                    val numDays = t.codec.hashLong(timestamp.toString, t.deterministic, passphrase) % t.threshold
                    state.updated(index, Timestamp.from(timestamp.toInstant.plus(numDays, ChronoUnit.DAYS)))

                  case (Some(t: Treatment.RandomString), StringType) =>
                    maskedAccumulator.add(1)
                    val string = row.getString(index)
                    state.updated(index, truncateString(encodeAsString(t.codec.hash(string, t.deterministic, passphrase), t.alphabet), t.length, t.codec.sparkString()))

                  case (Some(t: Treatment.RandomList), StringType) =>
                    maskedAccumulator.add(1)
                    val string = row.getString(index)
                    val localisedList = Mask.localizedLists.get(t.locale).get.get(t.list).get
                    state.updated(index, localisedList(Math.abs(t.codec.hashLong(string, t.deterministic, passphrase) % localisedList.length).toInt))

                  case (Some(t: Treatment.RandomEmail), StringType) =>
                    maskedAccumulator.add(1)
                    val string = row.getString(index)
                    val longBytes = t.codec.hashLong(string, t.deterministic, passphrase)

                    val first_name = Mask.localizedLists.get(t.locale).get.get("first_name_female").get ++ Mask.localizedLists.get(t.locale).get.get("first_name_male").get
                    val free_email = Mask.localizedLists.get(t.locale).get.get("free_email").get
                    val name = first_name(Math.abs(longBytes % first_name.length).toInt)
                    val emailDomain = free_email(Math.abs(longBytes % free_email.length).toInt)
                    state.updated(index, s"${name.toLowerCase}@$emailDomain")

                }
              }
              Row.fromSeq(masked).asInstanceOf[TransformedRow]
            }
          }
        }
      }
    } catch {
      case e: Exception => throw new Exception(e) with DetailException {
        override val detail = stage.stageDetail
      }
    }

    // re-attach metadata
    val outputDF = MetadataUtils.setMetadata(transformedDF, df.schema)

    if (arcContext.immutableViews) outputDF.createTempView(stage.outputView) else outputDF.createOrReplaceTempView(stage.outputView)

    if (!outputDF.isStreaming) {
      stage.stageDetail.put("outputColumns", java.lang.Integer.valueOf(outputDF.schema.length))
      stage.stageDetail.put("numPartitions", java.lang.Integer.valueOf(outputDF.rdd.partitions.length))

      if (stage.persist) {
        outputDF.persist(arcContext.storageLevel)
        stage.stageDetail.put("records", java.lang.Long.valueOf(outputDF.count))
        stage.stageDetail.put("maskedValues", java.lang.Long.valueOf(maskedAccumulator.value))
        stage.stageDetail.put("nonMaskedValues", java.lang.Long.valueOf(nonMaskedAccumulator.value))
      }
    }

    Option(outputDF)
  }

}
