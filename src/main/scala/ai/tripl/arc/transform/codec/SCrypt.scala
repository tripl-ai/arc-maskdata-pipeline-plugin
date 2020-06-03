package ai.tripl.arc.transform.codec

import scala.util.Properties._

import javax.crypto.SecretKeyFactory
import javax.crypto.spec.PBEKeySpec

import ai.tripl.arc.plugins.udf.MaskDataTransformCodec

class SCrypt extends MaskDataTransformCodec with Serializable {
  val DEFAULT_PARALLELISM = 1
  val DEFAULT_MEMORY = 8
  val DEFAULT_CPU = 16384

  val cpu = envOrNone("ETL_CONF_MASK_DATA_SCRYPT_CPU").map(_.toInt).getOrElse(DEFAULT_CPU)
  val memory = envOrNone("ETL_CONF_MASK_DATA_SCRYPT_MEMORY").map(_.toInt).getOrElse(DEFAULT_MEMORY)
  val parallelism = envOrNone("ETL_CONF_MASK_DATA_SCRYPT_PARALLELISM").map(_.toInt).getOrElse(DEFAULT_PARALLELISM)

  def sparkName() = "SCrypt"
  def sparkString() = s"SCrypt($cpu, $memory, $parallelism)"
  def getVersion() = "1.0.0"

  def encrypt(value: Array[Char], salt: Array[Byte]): Array[Byte] = {
    org.bouncycastle.crypto.generators.SCrypt.generate(
      value.map(_.toByte),
      salt,
      cpu,
      memory,
      parallelism,
      DEFAULT_HASH_LENGTH
    )
  }

}