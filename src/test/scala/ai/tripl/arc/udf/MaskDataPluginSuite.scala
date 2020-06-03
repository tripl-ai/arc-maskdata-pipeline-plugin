package ai.tripl.arc.plugins

import ai.tripl.arc.api.API._
import org.apache.spark.sql.SparkSession
import org.scalatest.{BeforeAndAfter, FunSuite}
import ai.tripl.arc.util.TestUtils

import ai.tripl.arc.udf.UDF
import ai.tripl.arc.ARC
import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.config._

class MaskDataPluginSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _
  var logger: ai.tripl.arc.util.log.logger.Logger = _
  val outputView = "outputView"

  before {
    implicit val spark = SparkSession
      .builder()
      .master("local[*]")
      .appName("Spark ETL Test")
      .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")

    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    session = spark

    implicit val logger = TestUtils.getLogger()
    val arcContext = TestUtils.getARCContext(isStreaming=false)

    // register udf
    UDF.registerUDFs()(spark,logger,arcContext)
  }

  after {
    session.stop()
  }

  test("MaskDataPluginSuite: mask string") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val conf = s"""{
      "stages": [
        {
          "type": "SQLTransform",
          "name": "test",
          "environments": [
            "production",
            "test"
          ],
          "sql": \"\"\"SELECT
            mask_string(16, true, null) AS mask_string_null
            ,mask_string(16, true, 'important') AS mask_string_determistic
            ,mask_string(16, false, 'important') AS mask_string_non_determistic
            ,mask_string_alphabet(16, '0123456789', true, 'important') AS mask_string_alphabet
          \"\"\",
          "outputView": "${outputView}",
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

  test("MaskDataPluginSuite: mask date") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val conf = s"""{
      "stages": [
        {
          "type": "SQLTransform",
          "name": "test",
          "environments": [
            "production",
            "test"
          ],
          "sql": \"\"\"SELECT
            mask_date(365, true, null) AS mask_date_null
            ,mask_date(365, true, date('2016-07-30')) AS mask_date_determistic
            ,mask_date(365, false, date('2016-07-30')) AS mask_date_non_determistic
          \"\"\",
          "outputView": "${outputView}",
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

  test("MaskDataPluginSuite: mask timestamp") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val conf = s"""{
      "stages": [
        {
          "type": "SQLTransform",
          "name": "test",
          "environments": [
            "production",
            "test"
          ],
          "sql": \"\"\"SELECT
            mask_timestamp(365, true, NULL) AS mask_timestamp_null
            ,mask_timestamp(365, true, timestamp('2016-07-30 22:23:45')) AS mask_timestamp_determistic
            ,mask_timestamp(365, false, timestamp('2016-07-30 22:23:45')) AS mask_timestamp_non_determistic
            ,date_trunc('HOUR', mask_timestamp(365, true, timestamp('2016-07-30 22:23:45'))) AS mask_timestamp_non_determistic_compose
            ,date_trunc('HOUR', mask_timestamp(365, false, timestamp('2016-07-30 22:23:45'))) AS mask_timestamp_determistic_compose
          \"\"\",
          "outputView": "${outputView}",
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

  test("MaskDataPluginSuite: mask decimal") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)

    val conf = s"""{
      "stages": [
        {
          "type": "SQLTransform",
          "name": "test",
          "environments": [
            "production",
            "test"
          ],
          "sql": \"\"\"SELECT
            mask_decimal(2.5, true, null) AS mask_decimal_null
            ,mask_decimal(2.5, true, CAST('23.45' AS DECIMAL(4, 2))) AS mask_decimal_deterministic
            ,mask_decimal(2.5, false, CAST('23.45' AS DECIMAL(4, 2))) AS mask_decimal_non_deterministic
            ,CAST(mask_decimal(2.5, false, CAST('23.45' AS DECIMAL(4, 2))) AS DECIMAL(4, 2)) AS mask_decimal_non_deterministic_cast
          \"\"\",
          "outputView": "${outputView}",
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
