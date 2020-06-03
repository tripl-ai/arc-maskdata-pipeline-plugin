import Dependencies._

lazy val scala211 = "2.11.12"
lazy val scala212 = "2.12.11"
lazy val supportedScalaVersions = List(scala211, scala212)

lazy val root = (project in file(".")).
  enablePlugins(BuildInfoPlugin).
  configs(IntegrationTest).
  settings(
    name := "arc-maskdata-pipeline-plugin",
    organization := "ai.tripl",
    organizationHomepage := Some(url("https://arc.tripl.ai")),
    crossScalaVersions := supportedScalaVersions,
    licenses := List("MIT" -> new URL("https://opensource.org/licenses/MIT")),
    scalastyleFailOnError := false,
    libraryDependencies ++= etlDeps,
    parallelExecution in Test := false,
    parallelExecution in IntegrationTest := false,
    fork in Test := true,
    envVars in Test := Map(
      "ETL_CONF_MASK_DATA_PASSPHRASE" -> "q7VqMTbj7e5dUWESfc8UjZr2r7pyq5cHP8MVkUEjyv74cHsqUt734vg6qQmHaz7a",
      "ETL_CONF_MASK_DATA_ARGON2_PARALLELISM" -> "2",
      "ETL_CONF_MASK_DATA_ARGON2_MEMORY" -> "16384",
      "ETL_CONF_MASK_DATA_ARGON2_ITERATIONS" -> "1",
      "ETL_CONF_MASK_DATA_PBKDF2_ITERATIONS" -> "131072",
      "ETL_CONF_MASK_DATA_SCRYPT_CPU" -> "32768",
      "ETL_CONF_MASK_DATA_SCRYPT_MEMORY" -> "16",
      "ETL_CONF_MASK_DATA_SCRYPT_PARALLELISM" -> "2"
    ),
    fork in Test := true,
    buildInfoKeys := Seq[BuildInfoKey](version, scalaVersion),
    buildInfoPackage := "ai.tripl.arc.maskdata",
    Defaults.itSettings,
    publishTo := sonatypePublishTo.value,
    pgpPassphrase := Some(sys.env.get("PGP_PASSPHRASE").getOrElse("").toCharArray),
    pgpSecretRing := file("/pgp/secring.asc"),
    pgpPublicRing := file("/pgp/pubring.asc"),
    updateOptions := updateOptions.value.withGigahorse(false),
    resolvers += "Sonatype OSS Staging" at "https://oss.sonatype.org/content/groups/staging"    
  )    


fork in run := true  

scalacOptions := Seq("-target:jvm-1.8", "-unchecked", "-deprecation")

test in assembly := {}

// META-INF discarding
assemblyMergeStrategy in assembly := {
   {
    // this match removes META-INF files except for the ones for plugins
    case PathList("META-INF", xs @ _*) =>
      xs match {
        case "services" :: xs => MergeStrategy.filterDistinctLines
        case _ => MergeStrategy.discard
      }
    case x => MergeStrategy.first
   }
}
