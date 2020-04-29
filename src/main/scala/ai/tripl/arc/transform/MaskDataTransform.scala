package ai.tripl.arc.transform

import java.net.URI
import java.nio.ByteBuffer
import java.security.SecureRandom
import java.sql.Date
import java.sql.Timestamp
import java.time.temporal.TemporalAdjusters._
import java.time.temporal.ChronoUnit
import java.time.ZoneId
import javax.crypto.SecretKeyFactory
import javax.crypto.spec.PBEKeySpec
import org.bouncycastle.crypto.generators._
import org.bouncycastle.crypto.params._
import org.apache.commons.codec.binary.Base32

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

class MaskDataTransform extends PipelineStagePlugin {

  val version = ai.tripl.arc.maskdata.BuildInfo.version

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val envVar = "ETL_CONF_MASK_DATA_PASSPHRASE"
    val signature = s"MaskDataTransform  environment variable '$envVar' must be a with a string with at least 64 characters."

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "outputView" :: "persist" :: "params" :: Nil
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val inputView = getValue[String]("inputView")
    val outputView = getValue[String]("outputView")
    val persist = getValue[java.lang.Boolean]("persist", default = Some(false))
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    // get the passphrase from an environment variable
    val passphrase = envOrNone(envVar) match {
      case Some(value) if (value.length < 64) => throw new Exception(s"${signature} Got ${value.length} characters.")
      case Some(value) => Option(value)
      case None => None
    }

    (name, description, inputView, outputView, persist, invalidKeys) match {
      case (Right(name), Right(description), Right(inputView), Right(outputView), Right(persist), Right(invalidKeys)) =>

        val stage = MaskDataTransformStage(
          plugin=this,
          name=name,
          description=description,
          inputView=inputView,
          outputView=outputView,
          persist=persist,
          params=params,
          passphrase=passphrase
        )

        stage.stageDetail.put("inputView", inputView)
        stage.stageDetail.put("outputView", outputView)
        stage.stageDetail.put("persist", java.lang.Boolean.valueOf(persist))

        Right(stage)
      case _ =>
        val allErrors: Errors = List(name, description, inputView, outputView, persist, invalidKeys).collect{ case Left(errs) => errs }.flatten
        val stageName = stringOrDefault(name, "unnamed stage")
        val err = StageError(index, stageName, c.origin.lineNumber, allErrors)
        Left(err :: Nil)
    }
  }
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
  def codec: Codec
}
object Treatment {
  val MASK_KEY = "mask"
  val TREATMENT_KEY = "treatment"
  val DEFAULT_SALT_BOOLEAN = """âbÆ¯h¸9Ü£¾m´¬ïAÎopúeýÊà]ú+^þ+úMÞV_òRþÖÞ¤"èôL_ç¾Æ;×s9ïÄeü3"#WqmM[""".getBytes
  val DEFAULT_SALT_DATE = """öRßÌé_{U+Ñôô@zäZiz`)î5<*óUûºfì-÷êâ3zÊ'-.Ìº5'G§zT:Z$ä¹C9ê½ü{7)p!{""".getBytes
  val DEFAULT_SALT_DECIMAL = """TÎA×GüMò-~Y×ã6Ö7\*¶.©}ÅM²5ûjMoÿLØ'ñydÍ°¤Jd`c(±@:]é¤Öó8Vç)m©]ØBæä""".getBytes
  val DEFAULT_SALT_DOUBLE = """93à/iL"8¢£")ñ·Ï#¥b\æ;Epr³å;mX\AAÒ°_¢)ãj8Õ>ü+½ü´Å:5ðú]ëy½;ÓÌæ5^ÌA""".getBytes
  val DEFAULT_SALT_INTEGER = """ªç9,V?`X&£fÒQÖÝ¤ëw/U9Q<á&_¹Âx\>»Ä^'ÜÑYóW7´©@Ûåó»(DF'm7u8:oÑcÆË>Á""".getBytes
  val DEFAULT_SALT_LONG = """aúhYÊê÷EGÙ`fG?óÌ"@öT^w)5Ýo}L¼È^i¬EQ¨ãÒ,V±¨¶=³\¶ç[à³>¼°Üf.EtAÖßgþ""".getBytes
  val DEFAULT_SALT_STRING = """i<ÜµUÐ©D§BYØbÃ±b+s'"ÇQÔË\6ÊUQKæåî%+¢;Gö§,övT¬Y$n7Ð£X¼,+&.Òú¶nRÜ)""".getBytes
  val DEFAULT_SALT_TIMESTAMP = """äÌµ¨ö5¹¨?çeg±&b}ãÐç';@Á$ÊL·e¾*ïðxÏû\dW9¦:ê#G;HéÍ2@ú-rbw9¼øÙUPÒt[""".getBytes

  // date treatments
  case class RandomDate(deterministic: Boolean, threshold: Long, codec: Codec, salt: Array[Byte]) extends CodecTreatment
  case class MonthDateTruncate() extends Treatment
  case class YearDateTruncate() extends Treatment

  // string treatments
  case class RandomString(deterministic: Boolean, length: Int, codec: Codec, salt: Array[Byte]) extends CodecTreatment
  case class RandomList(deterministic: Boolean, locale: String, list: String, codec: Codec, salt: Array[Byte]) extends CodecTreatment
  case class RandomEmail(deterministic: Boolean, locale: String, codec: Codec, salt: Array[Byte]) extends CodecTreatment

  def fromMetadata(field: StructField, passphrase: Option[String]): Option[Treatment] = {
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
                val deterministic = Try(mask.getBoolean("deterministic")) match {
                  case Success(value) => value
                  case Failure(_) => false
                }
                val threshold = mask.getLong("threshold")
                Option(Treatment.RandomDate(deterministic, threshold, Argon2(), DEFAULT_SALT_DATE))
              }
              case Success("monthDateTruncate") => {
                if (field.dataType != DateType && field.dataType != TimestampType) {
                  throw new Exception(s"Data Masking treatment 'monthDateTruncate' can only be applied to 'date' or 'timestamp' type but field '${field.name}' is of type '${field.dataType.simpleString}'.")
                }
                Option(Treatment.MonthDateTruncate())
              }
              case Success("yearDateTruncate") => {
                if (field.dataType != DateType && field.dataType != TimestampType) {
                  throw new Exception(s"Data Masking treatment 'yearDateTruncate' can only be applied to 'date' or 'timestamp' type but field '${field.name}' is of type '${field.dataType.simpleString}'.")
                }
                Option(Treatment.YearDateTruncate())
              }

              case Success("randomString") => {
                if (field.dataType != StringType) {
                  throw new Exception(s"Data Masking treatment 'randomString' can only be applied to 'string' type but field '${field.name}' is of type '${field.dataType.simpleString}'.")
                }
                val deterministic = Try(mask.getBoolean("deterministic")) match {
                  case Success(value) => value
                  case Failure(_) => false
                }
                val length = Try(mask.getLong("length")) match {
                  case Success(value) => value.toInt
                  case Failure(_) => 32
                }
                Option(Treatment.RandomString(deterministic, length, SCrypt(), DEFAULT_SALT_STRING))
              }
              case Success("randomList") => {
                if (field.dataType != StringType) {
                  throw new Exception(s"Data Masking treatment 'randomList' can only be applied to 'string' type but field '${field.name}' is of type '${field.dataType.simpleString}'.")
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
                Option(Treatment.RandomList(deterministic, locale, list, PBKDF2WithHmacSHA512(), DEFAULT_SALT_STRING))
              }
              case Success("randomEmail") => {
                if (field.dataType != StringType) {
                  throw new Exception(s"Data Masking treatment 'randomEmail' can only be applied to 'string' type but field '${field.name}' is of type '${field.dataType.simpleString}'.")
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
                Option(Treatment.RandomEmail(deterministic, locale, PBKDF2WithHmacSHA512(), DEFAULT_SALT_STRING))
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

sealed trait Codec {
  def encrypt(value: Array[Char], salt: Array[Byte]): Array[Byte]

  def hash(value: String, salt: Array[Byte], deterministic: Boolean, passphrase: Option[String]): Array[Byte] = {
    if (deterministic) {
      encrypt(value.toCharArray ++ passphrase.getOrElse(throw new Exception("MaskDataTransform environment variable 'ETL_CONF_MASK_DATA_PASSPHRASE' is required for deterministic data masking.")).getBytes.map(_.toChar), salt)
    } else {
      val secureRandom = new SecureRandom()
      val randomBytes = new Array[Byte](Codec.DEFAULT_HASH_LENGTH)
      secureRandom.nextBytes(randomBytes)
      randomBytes
    }
  }

  // ByteBuffer.wrap(bytes).getLong returns positive and negative values
  def hashLong(value: String, salt: Array[Byte], deterministic: Boolean, passphrase: Option[String]): Long = {
    ByteBuffer.wrap(hash(value, salt, deterministic, passphrase)).getLong
  }
}

object Codec {
  val DEFAULT_HASH_LENGTH = 64
}

case class PBKDF2WithHmacSHA512(iterations: Option[Int] = None, keyLength: Option[Int] = None) extends Codec {
	val DEFAULT_ITERATIONS = 262144
	val DEFAULT_KEY_LENGTH = 512

  def encrypt(value: Array[Char], salt: Array[Byte]): Array[Byte] = {
    val secretKeyFactory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA512")

    val keySpec = new PBEKeySpec(
      value,
      salt,
      iterations.getOrElse(DEFAULT_ITERATIONS),
      keyLength.getOrElse(DEFAULT_KEY_LENGTH)
    )

    val hash = secretKeyFactory.generateSecret(keySpec).getEncoded
    keySpec.clearPassword
    hash
  }
}

case class Argon2(parallelism: Option[Int] = None, memory: Option[Int] = None, iterations: Option[Int] = None) extends Codec {
	val DEFAULT_PARALLELISM = 1
	val DEFAULT_MEMORY = 1 << 12
  val DEFAULT_ITERATIONS = 3

  def encrypt(value: Array[Char], salt: Array[Byte]): Array[Byte] = {

    val params = new Argon2Parameters.Builder(Argon2Parameters.ARGON2_id)
      .withSalt(salt)
      .withParallelism(parallelism.getOrElse(DEFAULT_PARALLELISM))
      .withMemoryAsKB(memory.getOrElse(DEFAULT_MEMORY))
      .withIterations(iterations.getOrElse(DEFAULT_ITERATIONS))
      .build()

    val generator = new Argon2BytesGenerator()
    generator.init(params)
    val hash = new Array[Byte](Codec.DEFAULT_HASH_LENGTH)
    generator.generateBytes(value, hash)
    hash
  }
}

case class SCrypt(parallelism: Option[Int] = None, memory: Option[Int] = None, cpu: Option[Int] = None) extends Codec {
	val DEFAULT_PARALLELISM = 1
	val DEFAULT_MEMORY = 8
  val DEFAULT_CPU = 16384

  def encrypt(value: Array[Char], salt: Array[Byte]): Array[Byte] = {
    org.bouncycastle.crypto.generators.SCrypt.generate(value.map(_.toByte), salt, cpu.getOrElse(DEFAULT_CPU), memory.getOrElse(DEFAULT_MEMORY), parallelism.getOrElse(DEFAULT_PARALLELISM), Codec.DEFAULT_HASH_LENGTH)
  }
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

            // create an array of tuple(DataType, Option[Treatement to apply], field index to apply to)
            val treatments = schema.fields
              .zipWithIndex
              .map { case (field, index) => (field.dataType, Treatment.fromMetadata(field, passphrase), index) }

            // set up useful objects
            val base32 = new Base32()
            val secureRandom = new SecureRandom()
            val utcZoneId = ZoneId.of("UTC")

            bufferedPartition.map { row =>
              val masked = treatments.foldLeft[Seq[Any]](row.toSeq) { case (state, (dataType, treatment, index)) =>
                treatment match {

                  case None =>
                    nonMaskedAccumulator.add(1)
                    state

                  case Some(_) if row.isNullAt(index) =>
                    nonMaskedAccumulator.add(1)
                    state

                  case Some(_: Treatment.MonthDateTruncate) if dataType == DateType =>
                    maskedAccumulator.add(1)
                    state.updated(index, Date.valueOf(row.getDate(index).toLocalDate.withDayOfMonth(1)))

                  case Some(_: Treatment.MonthDateTruncate) if dataType == TimestampType =>
                    maskedAccumulator.add(1)
                    val timestamp = row.getTimestamp(index)
                    state.updated(index, Timestamp.from(timestamp.toInstant.atZone(utcZoneId).withDayOfMonth(1).toInstant))

                  case Some(_: Treatment.YearDateTruncate) if dataType == DateType =>
                    maskedAccumulator.add(1)
                    state.updated(index, Date.valueOf(row.getDate(index).toLocalDate.withDayOfMonth(1).withMonth(1)))

                  case Some(_: Treatment.YearDateTruncate) if dataType == TimestampType =>
                    maskedAccumulator.add(1)
                    val timestamp = row.getTimestamp(index)
                    state.updated(index, Timestamp.from(timestamp.toInstant.atZone(utcZoneId).withDayOfMonth(1).withMonth(1).toInstant))

                  case Some(t: Treatment.RandomDate) if dataType == DateType =>
                    maskedAccumulator.add(1)
                    val date = row.getDate(index)
                    val numDays = t.codec.hashLong(date.toString, t.salt, t.deterministic, passphrase) % t.threshold
                    state.updated(index, Date.valueOf(date.toLocalDate.plusDays(numDays)))

                  case Some(t: Treatment.RandomDate) if dataType == TimestampType =>
                    maskedAccumulator.add(1)
                    val timestamp = row.getTimestamp(index)
                    val numDays = t.codec.hashLong(timestamp.toString, t.salt, t.deterministic, passphrase) % t.threshold
                    state.updated(index, Timestamp.from(timestamp.toInstant.plus(numDays, ChronoUnit.DAYS)))

                  case Some(t: Treatment.RandomString) =>
                    maskedAccumulator.add(1)
                    val string = row.getString(index)
                    state.updated(index, base32.encodeAsString(t.codec.hash(string, t.salt, t.deterministic, passphrase)).substring(0, t.length))

                  case Some(t: Treatment.RandomList) =>
                    maskedAccumulator.add(1)
                    val string = row.getString(index)
                    val localisedList = Mask.localizedLists.get(t.locale).get.get(t.list).get
                    state.updated(index, localisedList(Math.abs(t.codec.hashLong(string, t.salt, t.deterministic, passphrase) % localisedList.length).toInt))

                  case Some(t: Treatment.RandomEmail) =>
                    maskedAccumulator.add(1)
                    val string = row.getString(index)
                    val longBytes = t.codec.hashLong(string, t.salt, t.deterministic, passphrase)

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
