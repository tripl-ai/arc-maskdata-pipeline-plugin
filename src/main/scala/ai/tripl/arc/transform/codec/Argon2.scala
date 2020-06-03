package ai.tripl.arc.transform.codec

import scala.util.Properties._

import org.bouncycastle.crypto.generators._
import org.bouncycastle.crypto.params._

import ai.tripl.arc.plugins.udf.MaskDataTransformCodec

class Argon2 extends MaskDataTransformCodec with Serializable {
  val DEFAULT_PARALLELISM = 2
  val DEFAULT_MEMORY = 65536
  val DEFAULT_ITERATIONS = 4

  val parallelism = envOrNone("ETL_CONF_MASK_DATA_ARGON2_PARALLELISM").map(_.toInt).getOrElse(DEFAULT_PARALLELISM)
  val memory = envOrNone("ETL_CONF_MASK_DATA_ARGON2_MEMORY").map(_.toInt).getOrElse(DEFAULT_MEMORY)
  val iterations = envOrNone("ETL_CONF_MASK_DATA_ARGON2_ITERATIONS").map(_.toInt).getOrElse(DEFAULT_ITERATIONS)

  def sparkName() = s"Argon2"
  def sparkString() = s"Argon2($parallelism, $memory, $iterations)"
  def getVersion() = "1.0.0"

  def encrypt(value: Array[Char], salt: Array[Byte]): Array[Byte] = {

    val params = new Argon2Parameters.Builder(Argon2Parameters.ARGON2_id)
      .withSalt(salt)
      .withParallelism(parallelism)
      .withMemoryAsKB(memory)
      .withIterations(iterations)
      .build()

    val generator = new Argon2BytesGenerator()
    generator.init(params)
    val hash = new Array[Byte](DEFAULT_HASH_LENGTH)
    generator.generateBytes(value, hash)
    hash
  }

}