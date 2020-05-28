package ai.tripl.arc.transform.codec

import scala.util.Properties._

import javax.crypto.SecretKeyFactory
import javax.crypto.spec.PBEKeySpec

import ai.tripl.arc.transform.MaskDataTransformCodec

class PBKDF2WithHmacSHA512Codec extends MaskDataTransformCodec {
  val DEFAULT_ITERATIONS = 262144
  
  val iterationCount = envOrNone("ETL_CONF_MASK_DATA_PBKDF2_ITERATIONS").map(_.toInt).getOrElse(DEFAULT_ITERATIONS)
  val keyLength = envOrNone("ETL_CONF_MASK_DATA_PBKDF2_KEY_LENGTH").map(_.toInt).getOrElse(MaskDataTransformCodec.DEFAULT_HASH_LENGTH)

  def sparkName() = "PBKDF2WithHmacSHA512"
  def sparkString() = s"PBKDF2WithHmacSHA512($iterationCount, $keyLength)"
  def getVersion() = "1.0.0"

  def encrypt(value: Array[Char], salt: Array[Byte]): Array[Byte] = {
    val secretKeyFactory = SecretKeyFactory.getInstance("PBKDF2WithHmacSHA512")

    val keySpec = new PBEKeySpec(
      value,
      salt,
      iterationCount,
      keyLength * 8
    )

    val hash = secretKeyFactory.generateSecret(keySpec).getEncoded
    keySpec.clearPassword
    hash
  }  
  
}