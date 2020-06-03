package ai.tripl.arc

import java.net.URI
import java.sql.Date
import java.util.Calendar
import java.util.TimeZone
import java.sql.Timestamp
import java.time._

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import ai.tripl.arc.util.log.LoggerFactory

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.config._
import ai.tripl.arc.util._
import ai.tripl.arc.udf.UDF

class MaskDataTransformSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _
  var logger: ai.tripl.arc.util.log.logger.Logger = _

  val inputView = "inputView"
  val outputView = "outputView"
  val schemaView = "schemaView"
  val tz = TimeZone.getTimeZone("UTC")

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")
    logger = TestUtils.getLogger()

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    session = spark
    import spark.implicits._

    val arcContext = TestUtils.getARCContext(isStreaming=false)

    UDF.registerUDFs()(spark, logger, arcContext)

  }

  after {
    session.stop()
  }

  test("MaskDataTransform: end-to-end") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val conf = s"""{
      "stages": [
        {
          "type": "DelimitedExtract",
          "name": "test",
          "description": "test",
          "environments": [
            "production",
            "test"
          ],
          "inputURI": "${getClass.getResource("/userdata.csv").toString}",
          "outputView": "userdata_raw",
          "header": true
        },
        {
          "type": "TypingTransform",
          "name": "test",
          "description": "test",
          "environments": [
            "production",
            "test"
          ],
          "schemaURI": "${getClass.getResource("/userdata.json").toString}",
          "inputView": "userdata_raw",
          "outputView": "userdata",
          "persist": true
        },
        {
          "type": "MaskDataTransform",
          "name": "mask data",
          "environments": [
            "production",
            "test"
          ],
          "inputView": "userdata",
          "outputView": "userdata_masked",
          "persist": true
        }          
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => {
        val df = ARC.run(pipeline)(spark, logger, arcContext).get
        df.show(false)
      }
    }
  }      

}
