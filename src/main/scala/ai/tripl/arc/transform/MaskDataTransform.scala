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
import ai.tripl.arc.util.SQLUtils
import ai.tripl.arc.util.Utils

class MaskDataTransform extends PipelineStagePlugin {

  val version = ai.tripl.arc.maskdata.BuildInfo.version

  def instantiate(index: Int, config: com.typesafe.config.Config)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Either[List[ai.tripl.arc.config.Error.StageError], PipelineStage] = {
    import ai.tripl.arc.config.ConfigReader._
    import ai.tripl.arc.config.ConfigUtils._
    implicit val c = config

    val expectedKeys = "type" :: "name" :: "description" :: "environments" :: "inputView" :: "outputView" :: "persist" :: "params" :: Nil
    val name = getValue[String]("name")
    val description = getOptionalValue[String]("description")
    val inputView = getValue[String]("inputView")
    val outputView = getValue[String]("outputView")
    val persist = getValue[java.lang.Boolean]("persist", default = Some(false))
    val params = readMap("params", c)
    val invalidKeys = checkValidKeys(c)(expectedKeys)

    (name, description, inputView, outputView, persist, invalidKeys) match {
      case (Right(name), Right(description), Right(inputView), Right(outputView), Right(persist), Right(invalidKeys)) =>

        val stage = MaskDataTransformStage(
          plugin=this,
          name=name,
          description=description,
          inputView=inputView,
          outputView=outputView,
          persist=persist,
          params=params
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
    params: Map[String, String]
  ) extends PipelineStage {

  override def execute()(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    MaskDataTransformStage.execute(this)
  }
}

object MaskDataTransformStage {
  val MASK_KEY = "mask"
  val TREATMENTS_KEY = "treatments"

  def execute(stage: MaskDataTransformStage)(implicit spark: SparkSession, logger: ai.tripl.arc.util.log.logger.Logger, arcContext: ARCContext): Option[DataFrame] = {
    import spark.implicits._

    val df = spark.table(stage.inputView)
    
    val stmt = df.schema.fields.map { field =>
      if (field.metadata.contains(MASK_KEY)) {
        Try(field.metadata.getMetadata(MASK_KEY)) match {
          case Success(mask) => {
            Try(mask.getStringArray(TREATMENTS_KEY)) match {
              case Success(treatments) => {
                val functions = treatments.foldLeft("") { (state, treatment) =>
                  if (state == "") {
                    SQLUtils.injectParameters(treatment, Map("value" -> field.name), false)
                  } else {
                    SQLUtils.injectParameters(treatment, Map("value" -> state), false)
                  }
                }
                field.dataType match {
                  case decimal: DecimalType => s"CAST($functions AS DECIMAL(${decimal.precision}, ${decimal.scale})) AS ${field.name}"
                  case date: DateType => s"CAST($functions AS DATE) AS ${field.name}"
                  case _ => s"$functions AS ${field.name}"
                }
              }
              case Failure(_) => throw new Exception(s"field '${field.name}' is missing treatments key '${TREATMENTS_KEY}'")
            }
          }
          case Failure(_) => throw new Exception(s"field '${field.name}' has invalid type for '${MASK_KEY}'.")
        }
      } else {
        field.name
      }
    }.mkString("SELECT ", ", ", s" FROM ${stage.inputView}")

    println(stmt)
    
    stage.stageDetail.put("sql", stmt)

    var transformedDF = try {
      spark.sql(stmt)
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
        val startTime = System.currentTimeMillis()
        outputDF.persist(arcContext.storageLevel)
        val count = outputDF.count
        val endTime = System.currentTimeMillis()
        stage.stageDetail.put("records", java.lang.Long.valueOf(count))
        stage.stageDetail.put("rate", java.lang.Long.valueOf((endTime-startTime)/count))
      }
    }

    Option(outputDF)
  }

}
