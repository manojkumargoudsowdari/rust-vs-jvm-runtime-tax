ThisBuild / scalaVersion := "3.5.0"
ThisBuild / organization := "rvj"

lazy val root = (project in file("."))
  .settings(
    name := "pipeline-scala",
    version := "0.1.0",
    libraryDependencies ++= Seq(
      // parquet-java; declares Hadoop as `provided`, so we supply the shaded clients.
      "org.apache.parquet" % "parquet-hadoop" % "1.14.4",
      "org.apache.hadoop" % "hadoop-client-api" % "3.3.6",
      "org.apache.hadoop" % "hadoop-client-runtime" % "3.3.6",
      "org.slf4j" % "slf4j-nop" % "2.0.16",
      "org.scalameta" %% "munit" % "1.0.2" % Test
    ),
    scalacOptions ++= Seq("-deprecation", "-feature", "-release", "21"),
    Compile / mainClass := Some("rvj.Main"),
    assembly / assemblyJarName := "pipeline-scala.jar",
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", _*) => MergeStrategy.concat
      case PathList("META-INF", _*)             => MergeStrategy.discard
      case "module-info.class"                  => MergeStrategy.discard
      case _                                    => MergeStrategy.first
    }
  )
