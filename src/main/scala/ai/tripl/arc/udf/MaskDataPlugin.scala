package ai.tripl.arc.plugins.udf

import java.nio.ByteBuffer
import java.security.SecureRandom
import java.util.ServiceLoader
import java.sql.Date
import java.sql.Timestamp
import java.time.temporal.ChronoUnit

import scala.collection.JavaConverters._
import scala.util.Properties._

import org.apache.spark.sql._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

import ai.tripl.arc.util.log.logger.Logger
import ai.tripl.arc.api.API.ARCContext
import ai.tripl.arc.util.Utils
import ai.tripl.arc.util.ControlUtils.using
import ai.tripl.arc.transform.codec.Argon2


class MaskDataPlugin extends ai.tripl.arc.plugins.UDFPlugin {

  val version = Utils.getFrameworkVersion

  // one udf plugin can register multiple user defined functions
  override def register()(implicit spark: SparkSession, logger: Logger, arcContext: ARCContext) = {
    val signature = s"MasvkDataTransform environment variable 'ETL_CONF_MASK_DATA_PASSPHRASE' must be a string of between 64 and 256 characters."

    // get the passphrase from an environment variable
    val passphrase = envOrNone("ETL_CONF_MASK_DATA_PASSPHRASE") match {
      case Some(value) if (value.length < 64) || (value.length > 256) => throw new Exception(signature)
      case Some(value) => value.getBytes
      case None => throw new Exception(signature)
    }

    // load codecs. these are not serializable so have to be re-initalised on each executor but are useful for logging
    val serviceLoader = Utils.getContextOrSparkClassLoader
    val maskDataTransformCodecs = ServiceLoader.load(classOf[MaskDataTransformCodec], serviceLoader).iterator().asScala.toList
    if (maskDataTransformCodecs.length == 0) {
      throw new Exception("No codecs found to perform deterministic data masking.")
    }
    // get the passphrase from an environment variable
    val codecName = envOrElse("ETL_CONF_MASK_DATA_CODEC", "Argon2")
    val codec = maskDataTransformCodecs.filter { codec => codec.sparkName() == codecName }.headOption match {
      case Some(value) => value
      case None => throw new Exception(s"'ETL_CONF_MASK_DATA_CODEC' codec '${codecName}' not found.")
    }

    logger.info()
      .field("event", "register")
      .field("codec", codec.sparkString)
      .field("codecs", maskDataTransformCodecs.map { codec => s"${codec.getClass.getName}:${codec.getVersion}" }.asJava)
      .log()

    // register custom UDFs via sqlContext.udf.register("funcName", func )
    spark.sqlContext.udf.register("mask_string", (length: Int, deterministic: Boolean, value: String) => MaskDataPlugin.mask_string(codec, passphrase)(length, "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ", None, deterministic, value) )
    spark.sqlContext.udf.register("mask_string_alphabet", (length: Int, alphabet: String, deterministic: Boolean, value: String) => MaskDataPlugin.mask_string(codec, passphrase)(length, alphabet, None, deterministic, value) )
    spark.sqlContext.udf.register("mask_string_alphabet_format", (length: Int, alphabet: String, format: String, deterministic: Boolean, value: String) => MaskDataPlugin.mask_string(codec, passphrase)(length, alphabet, Some(format), deterministic, value) )
    spark.sqlContext.udf.register("mask_date", (range: Int, deterministic: Boolean, value: Date) => MaskDataPlugin.mask_date(codec, passphrase)(range, deterministic, value) )
    spark.sqlContext.udf.register("mask_timestamp", (range: Int, deterministic: Boolean, value: Timestamp) => MaskDataPlugin.mask_timestamp(codec, passphrase)(range, deterministic, value) )
    spark.sqlContext.udf.register("mask_decimal", (range: java.math.BigDecimal, deterministic: Boolean, value: java.math.BigDecimal) => MaskDataPlugin.mask_decimal(codec, passphrase)(range, deterministic, value))
  }

}

object MaskDataPlugin {
  def encodeAsString(bytes: Array[Byte], alphabet: String) = bytes.map { byte => alphabet(Math.abs(byte.toInt) % alphabet.length) }.mkString
  def truncateString(value: String, length: Int, algorithm: String): String = {
    if (value.length < length) {
      throw new Exception(s"'${algorithm}' cannot produce required ${length} length output based on the input")
    }
    value.substring(0, length)
  }    

  def mask_string(codec: MaskDataTransformCodec, passphrase: Array[Byte]) = (length: Int, alphabet: String, format: Option[String], deterministic: Boolean, value: String) => {
    Option(value) match {
      case Some(value) => {
        format match {
          case Some(format) => {
            // to do this will take format like 4xxx-xxxx-xxxx-xxxx + number alphabet to generate a credit-card like string
            truncateString(encodeAsString(codec.hash(value, deterministic, passphrase), alphabet), 16, codec.sparkName())
          }
          case None => truncateString(encodeAsString(codec.hash(value, deterministic, passphrase), alphabet), 16, codec.sparkName())
        }
      }
      case None => null
    }  
  }

  def mask_date(codec: MaskDataTransformCodec, passphrase: Array[Byte]) = (range: Int, deterministic: Boolean, value: Date) => {
    Option(value) match {
      case Some(value) => {
        val numDays = (codec.hashLong(value.toString, deterministic, passphrase) % range).toInt
        Date.valueOf(value.toLocalDate.plusDays(numDays))
      }
      case None => null
    }
  }  

  def mask_timestamp(codec: MaskDataTransformCodec, passphrase: Array[Byte]) = (range: Int, deterministic: Boolean, value: Timestamp) => {
    Option(value) match {
      case Some(value) => {
        val numDays = (codec.hashLong(value.toString, deterministic, passphrase) % range).toInt
        Timestamp.from(value.toInstant.plus(numDays, ChronoUnit.DAYS))
      }
      case None => null
    }
  }

  def mask_decimal(codec: MaskDataTransformCodec, passphrase: Array[Byte]) = (range: java.math.BigDecimal, deterministic: Boolean, value: java.math.BigDecimal) => {
    Option(value) match {
      case Some(value) => {
        val randomDecimal = BigDecimal(ByteBuffer.wrap(codec.hash(value.toString, deterministic, passphrase)).getLong) / Math.pow(10, value.scale)
        (BigDecimal(value) + (randomDecimal % BigDecimal(range))).bigDecimal
      }
      case None => null
    }
  }

}

trait MaskDataTransformCodec {
  val DEFAULT_HASH_LENGTH = 64
  
  def sparkName(): String
  def sparkString(): String
  def getVersion(): String

  def encrypt(value: Array[Char], salt: Array[Byte]): Array[Byte]

  def hash(value: String, deterministic: Boolean, passphrase: Array[Byte]): Array[Byte] = {
    if (deterministic) {
      val (pass, salt) = passphrase.splitAt((passphrase.length / 2))
      encrypt(
        value.toCharArray ++ pass.map(_.toChar),
        salt
      )
    } else {
      val secureRandom = new SecureRandom()
      val randomBytes = new Array[Byte](DEFAULT_HASH_LENGTH)
      secureRandom.nextBytes(randomBytes)
      randomBytes
    }
  }

  // ByteBuffer.wrap(bytes).getLong returns positive and negative values
  def hashLong(value: String, deterministic: Boolean, passphrase: Array[Byte]): Long = {
    ByteBuffer.wrap(hash(value, deterministic, passphrase)).getLong
  }
}

