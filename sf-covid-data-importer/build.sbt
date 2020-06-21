ThisBuild / scalaVersion     := "2.13.2"
ThisBuild / version          := "0.1.0-SNAPSHOT"
ThisBuild / organization     := "com.akafka"
ThisBuild / organizationName := "example"

val circeVersion = "0.12.3"

lazy val root = (project in file("."))
  .settings(
    name := "sf-covid-data-importer",
    scalacOptions += "-Ymacro-annotations",
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.1.1",
      "com.typesafe.akka" %% "akka-http"   % "10.1.12",
      "com.typesafe.akka" %% "akka-stream" % "2.5.26",
      "io.circe" %% "circe-core" % circeVersion,
      "io.circe" %% "circe-generic" % circeVersion,
      "io.circe" %% "circe-parser" % circeVersion,
      "io.circe" %% "circe-generic-extras" % "0.13.0",
      )
  )
