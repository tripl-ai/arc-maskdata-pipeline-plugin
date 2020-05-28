package ai.tripl.arc

import java.net.URI
import java.sql.Date
import java.sql.Timestamp
import java.time._

import org.scalatest.FunSuite
import org.scalatest.BeforeAndAfter

import org.apache.spark.sql._
import org.apache.spark.sql.types._

import ai.tripl.arc.api._
import ai.tripl.arc.api.API._
import ai.tripl.arc.config._
import ai.tripl.arc.util._

class MaskDataTransformSuite extends FunSuite with BeforeAndAfter {

  var session: SparkSession = _
  val inputView = "inputView"
  val outputView = "outputView"
  val schemaView = "schemaView"

  before {
    implicit val spark = SparkSession
                  .builder()
                  .master("local[*]")
                  .config("spark.ui.port", "9999")
                  .appName("Spark ETL Test")
                  .getOrCreate()
    spark.sparkContext.setLogLevel("INFO")


    // set for deterministic timezone
    spark.conf.set("spark.sql.session.timeZone", "UTC")

    session = spark
    import spark.implicits._
  }

  after {
    session.stop()
  }

  test("MaskDataTransformSuite: test date functions") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)
    import spark.implicits._

    val threshold = 365L

    val data = Seq(
      Row(0,
      Date.valueOf("2016-12-18"),
      Date.valueOf("2016-12-18"),
      null,
      Timestamp.from(ZonedDateTime.of(2016, 12, 18, 21, 46, 54, 0, ZoneId.of("UTC")).toInstant),
      null,
      Date.valueOf("2016-12-18"),
      null,
      Date.valueOf("2016-12-18"),
      null,
      Date.valueOf("2016-12-18"),
      null
      )
    )
    val schema = StructType(
      List(
        StructField("id", IntegerType, true),
        StructField("raw", DateType, true),
        StructField("monthDateTruncate", DateType, true, new MetadataBuilder().putMetadata("mask", new MetadataBuilder().putString("treatment", "monthDateTruncate").build()).build()),
        StructField("monthDateTruncateNull", DateType, true, new MetadataBuilder().putMetadata("mask", new MetadataBuilder().putString("treatment", "monthDateTruncate").build()).build()),
        StructField("monthTimestampTruncate", TimestampType, true, new MetadataBuilder().putMetadata("mask", new MetadataBuilder().putString("treatment", "monthDateTruncate").build()).build()),
        StructField("monthTimestampTruncateNull", TimestampType, true, new MetadataBuilder().putMetadata("mask", new MetadataBuilder().putString("treatment", "monthDateTruncate").build()).build()),
        StructField("yearDateTruncate", DateType, true, new MetadataBuilder().putMetadata("mask", new MetadataBuilder().putString("treatment", "yearDateTruncate").build()).build()),
        StructField("yearDateTruncateNull", DateType, true, new MetadataBuilder().putMetadata("mask", new MetadataBuilder().putString("treatment", "yearDateTruncate").build()).build()),
        StructField("deterministicRandomDate", DateType, true, new MetadataBuilder().putMetadata("mask", new MetadataBuilder().putString("treatment", "randomDate").putBoolean("deterministic", true).putLong("threshold", threshold).build()).build()),
        StructField("deterministicRandomDateNull", DateType, true, new MetadataBuilder().putMetadata("mask", new MetadataBuilder().putString("treatment", "randomDate").putBoolean("deterministic", true).putLong("threshold", threshold).build()).build()),
        StructField("nonDeterministicRandomDate", DateType, true, new MetadataBuilder().putMetadata("mask", new MetadataBuilder().putString("treatment", "randomDate").putBoolean("deterministic", false).putLong("threshold", threshold).build()).build()),
        StructField("nonDeterministicRandomDateNull", DateType, true, new MetadataBuilder().putMetadata("mask", new MetadataBuilder().putString("treatment", "randomDate").putBoolean("deterministic", false).putLong("threshold", threshold).build()).build())
      )
    )

    val inputDF = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    inputDF.createOrReplaceTempView(inputView)

    val conf = s"""{
      "stages": [
        {
          "type": "MaskDataTransform",
          "name": "mask data",
          "environments": [
            "production",
            "test"
          ],
          "inputView": "${inputView}",
          "outputView": "${outputView}",
          "persist": true
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => {
        ARC.run(pipeline)(spark, logger, arcContext) match {
          case Some(df) => {
            val row = df.filter("id = 0").first
            assert(row.getDate(row.fieldIndex("raw")).toString == "2016-12-18")
            assert(row.getDate(row.fieldIndex("monthDateTruncate")).toString == "2016-12-01")
            assert(row.getTimestamp(row.fieldIndex("monthTimestampTruncate")) == Timestamp.from(ZonedDateTime.of(2016, 12, 1, 21, 46, 54, 0, ZoneId.of("UTC")).toInstant))
            assert(row.getDate(row.fieldIndex("yearDateTruncate")).toString == "2016-01-01")
            assert(row.getDate(row.fieldIndex("deterministicRandomDate")).toString == "2017-04-23")
            assert(row.getDate(row.fieldIndex("nonDeterministicRandomDate")).toLocalDate.isAfter(row.getDate(row.fieldIndex("nonDeterministicRandomDate")).toLocalDate.plusDays(threshold * -1)))
            assert(row.getDate(row.fieldIndex("nonDeterministicRandomDate")).toLocalDate.isBefore(row.getDate(row.fieldIndex("nonDeterministicRandomDate")).toLocalDate.plusDays(threshold)))
          }
          case None => assert(false)
        }
      }
    }
  }

  test("MaskDataTransformSuite: test string functions") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)
    import spark.implicits._

    val length = 16

    val data = Seq(
      Row(0,"John","John","John",null,"John","John","john.smith@tripl.ai","john.smith@tripl.ai","john.smith@tripl.ai","John","John")
    )
    val schema = StructType(
      List(
        StructField("id", IntegerType, true),
        StructField("raw", StringType, true),
        StructField("randomStringDeterministic", StringType, true, new MetadataBuilder().putMetadata("mask", new MetadataBuilder().putString("treatment", "randomString").putBoolean("deterministic", true).putLong("length", length).build()).build()),
        StructField("randomStringNonDeterministic", StringType, true, new MetadataBuilder().putMetadata("mask", new MetadataBuilder().putString("treatment", "randomString").putBoolean("deterministic", false).putLong("length", length).build()).build()),
        StructField("randomStringNull", StringType, true, new MetadataBuilder().putMetadata("mask", new MetadataBuilder().putString("treatment", "randomString").build()).build()),
        StructField("randomNameDeterministic", StringType, true, new MetadataBuilder().putMetadata("mask", new MetadataBuilder().putString("treatment", "randomList").putString("locale", "en-AU").putString("list", "first_name_female").putBoolean("deterministic", true).build()).build()),
        StructField("randomNameNonDeterministic", StringType, true, new MetadataBuilder().putMetadata("mask", new MetadataBuilder().putString("treatment", "randomList").putString("locale", "en-AU").putString("list", "first_name_female").putBoolean("deterministic", false).build()).build()),
        StructField("rawEmail", StringType, true),
        StructField("randomEmailDeterministic", StringType, true, new MetadataBuilder().putMetadata("mask", new MetadataBuilder().putString("treatment", "randomEmail").putString("locale", "en-AU").putBoolean("deterministic", true).build()).build()),
        StructField("randomEmailNonDeterministic", StringType, true, new MetadataBuilder().putMetadata("mask", new MetadataBuilder().putString("treatment", "randomEmail").putString("locale", "en-AU").putBoolean("deterministic", false).build()).build()),
        StructField("randomNumberStringDeterministic", StringType, true, new MetadataBuilder().putMetadata("mask", new MetadataBuilder().putString("treatment", "randomNumberString").putBoolean("deterministic", true).putLong("length", length).build()).build()),
        StructField("randomNumberStringNonDeterministic", StringType, true, new MetadataBuilder().putMetadata("mask", new MetadataBuilder().putString("treatment", "randomNumberString").putBoolean("deterministic", false).putLong("length", length).build()).build())
      )
    )

    val inputDF = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    inputDF.createOrReplaceTempView(inputView)

    val conf = s"""{
      "stages": [
        {
          "type": "MaskDataTransform",
          "name": "mask data",
          "environments": [
            "production",
            "test"
          ],
          "inputView": "${inputView}",
          "outputView": "${outputView}",
          "persist": true
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => {
        ARC.run(pipeline)(spark, logger, arcContext) match {
          case Some(df) => {
            val row = df.filter("id = 0").first
            assert(row.getString(row.fieldIndex("raw")) == "John")
            assert(row.getString(row.fieldIndex("randomStringDeterministic")) == "WqXznQinfeVhAmQI")
            assert(row.getString(row.fieldIndex("randomNameDeterministic")) == "Tennessee")
            assert(Mask.localizedLists.get("en-AU").get.get("first_name_female").get.contains(row.getString(row.fieldIndex("randomNameNonDeterministic"))))
            assert(row.getString(row.fieldIndex("randomEmailDeterministic")) == "natalee@gmail.com")
            assert(row.getString(row.fieldIndex("randomNumberStringDeterministic")) == "0897520796718646")
          }
          case None => assert(false)
        }
      }
    }
  }

  test("MaskDataTransformSuite: test algorithm selection Argon2") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)
    import spark.implicits._

    val length = 16

    val data = Seq(
      Row(0,"John","John")
    )
    val schema = StructType(
      List(
        StructField("id", IntegerType, true),
        StructField("raw", StringType, true),
        StructField("randomStringDeterministicArgon2", StringType, true, new MetadataBuilder().putMetadata("mask", new MetadataBuilder().putString("algorithm", "Argon2").putString("treatment", "randomString").putBoolean("deterministic", true).putLong("length", length).build()).build()),
      )
    )

    val inputDF = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    inputDF.createOrReplaceTempView(inputView)

    val conf = s"""{
      "stages": [
        {
          "type": "MaskDataTransform",
          "name": "mask data",
          "environments": [
            "production",
            "test"
          ],
          "inputView": "${inputView}",
          "outputView": "${outputView}",
          "persist": true
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => {
        ARC.run(pipeline)(spark, logger, arcContext) match {
          case Some(df) => {
            val row = df.filter("id = 0").first
            assert(row.getString(row.fieldIndex("raw")) == "John")
            assert(row.getString(row.fieldIndex("randomStringDeterministicArgon2")) == "WqXznQinfeVhAmQI")
          }
          case None => assert(false)
        }
      }
    }
  }

  test("MaskDataTransformSuite: test algorithm selection PBKDF2") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)
    import spark.implicits._

    val length = 16

    val data = Seq(
      Row(0,"John","John")
    )
    val schema = StructType(
      List(
        StructField("id", IntegerType, true),
        StructField("raw", StringType, true),
        StructField("randomStringDeterministicPBKDF2", StringType, true, new MetadataBuilder().putMetadata("mask", new MetadataBuilder().putString("algorithm", "PBKDF2WithHmacSHA512").putString("treatment", "randomString").putBoolean("deterministic", true).putLong("length", length).build()).build()),
      )
    )

    val inputDF = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    inputDF.createOrReplaceTempView(inputView)

    val conf = s"""{
      "stages": [
        {
          "type": "MaskDataTransform",
          "name": "mask data",
          "environments": [
            "production",
            "test"
          ],
          "inputView": "${inputView}",
          "outputView": "${outputView}",
          "persist": true
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => {
        ARC.run(pipeline)(spark, logger, arcContext) match {
          case Some(df) => {
            val row = df.filter("id = 0").first
            assert(row.getString(row.fieldIndex("raw")) == "John")
            assert(row.getString(row.fieldIndex("randomStringDeterministicPBKDF2")) == "dlYzgoGslvQtiuHV")
          }
          case None => assert(false)
        }
      }
    }
  } 
  
 
 test("MaskDataTransformSuite: test algorithm selection SCrypt") {
    implicit val spark = session
    implicit val logger = TestUtils.getLogger()
    implicit val arcContext = TestUtils.getARCContext(isStreaming=false)
    import spark.implicits._

    val length = 16

    val data = Seq(
      Row(0,"John","John")
    )
    val schema = StructType(
      List(
        StructField("id", IntegerType, true),
        StructField("raw", StringType, true),
        StructField("randomStringDeterministicSCrypt", StringType, true, new MetadataBuilder().putMetadata("mask", new MetadataBuilder().putString("algorithm", "SCrypt").putString("treatment", "randomString").putBoolean("deterministic", true).putLong("length", length).build()).build()),
      )
    )

    val inputDF = spark.createDataFrame(spark.sparkContext.parallelize(data), schema)
    inputDF.createOrReplaceTempView(inputView)

    val conf = s"""{
      "stages": [
        {
          "type": "MaskDataTransform",
          "name": "mask data",
          "environments": [
            "production",
            "test"
          ],
          "inputView": "${inputView}",
          "outputView": "${outputView}",
          "persist": true
        }
      ]
    }"""

    val pipelineEither = ArcPipeline.parseConfig(Left(conf), arcContext)

    pipelineEither match {
      case Left(err) => fail(err.toString)
      case Right((pipeline, _)) => {
        ARC.run(pipeline)(spark, logger, arcContext) match {
          case Some(df) => {
            val row = df.filter("id = 0").first
            assert(row.getString(row.fieldIndex("raw")) == "John")
            assert(row.getString(row.fieldIndex("randomStringDeterministicSCrypt")) == "pVeXZdSnfMNEbfcq")
          }
          case None => assert(false)
        }
      }
    }
  }  

}
