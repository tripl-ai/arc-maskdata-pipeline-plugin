import sbt._

object Dependencies {
  // versions
  lazy val sparkVersion = "2.4.5"

  // testing
  val scalaTest = "org.scalatest" %% "scalatest" % "3.0.7" % "test,it"

  // arc
  val arc = "ai.tripl" %% "arc" % "2.12.5" % "provided"
  val typesafeConfig = "com.typesafe" % "config" % "1.3.1" intransitive()  

  // spark
  val sparkSql = "org.apache.spark" %% "spark-sql" % sparkVersion % "provided"

  // crypto library for MaskDataTransform
  val crypto = "org.bouncycastle" % "bcpkix-jdk15on" % "1.64"

  // Project
  val etlDeps = Seq(
    scalaTest,
    
    arc,
    typesafeConfig,

    sparkSql,

    crypto
  )
}