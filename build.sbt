
version := "1.0.0"
name := "razorlabs-app"
organization in ThisBuild := "com.razorlabs"

scalaVersion := "2.12.10"
val sparkVersion = "3.0.1"
val jacksonVersion = "2.12.0"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.logging.log4j" % "log4j-api" % "2.12.1",
  "org.apache.logging.log4j" % "log4j-core" % "2.12.1",
  "com.databricks" %% "spark-xml" % "0.11.0",
  "com.fasterxml.jackson.core" % "jackson-annotations" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-core" % jacksonVersion,
  "com.fasterxml.jackson.core" % "jackson-databind" % jacksonVersion,
  "com.fasterxml.jackson.module" %% "jackson-module-scala" % jacksonVersion,
  "com.fasterxml.jackson.dataformat" % "jackson-dataformat-yaml" % jacksonVersion,
  "org.scalatest" %% "scalatest" % "3.2.2" % Test
)

javaOptions in(Test, run) ++= Seq("" +
  "-Dspark.master=local",
  "-Dlog4j.debug=true")

lazy val app = (project in file(".")).
  settings(
    mainClass in assembly := Some("com.razorlabs.Main"),
    resourceDirectory in Compile := file(".") / "./src/main/resources",
    resourceDirectory in Runtime := file(".") / "./src/main/resources"
  )