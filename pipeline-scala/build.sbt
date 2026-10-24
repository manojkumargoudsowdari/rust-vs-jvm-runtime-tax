ThisBuild / scalaVersion := "3.5.0"
ThisBuild / organization := "rvj"

def isSignature(f: String): Boolean =
  Seq(".SF", ".DSA", ".RSA").exists(ext => f.toUpperCase.endsWith(ext))

def isLegalNotice(f: String): Boolean =
  Seq("LICENSE", "NOTICE").exists(f.toUpperCase.startsWith)

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
    // Explicit merge policy: anything not listed must be byte-identical across
    // jars (`deduplicate` fails the build on a content conflict).
    assembly / assemblyMergeStrategy := {
      case PathList("META-INF", "services", _*)                => MergeStrategy.filterDistinctLines
      case PathList("META-INF", "MANIFEST.MF")                 => MergeStrategy.discard
      case PathList("META-INF", f) if isSignature(f)           => MergeStrategy.discard
      // Legal notices differ per jar; keep every copy (renamed per source jar).
      case PathList("META-INF", f) if isLegalNotice(f)         => MergeStrategy.rename
      // Embedded pom.xml/pom.properties are build metadata, never read at runtime.
      case PathList("META-INF", "maven", _*)                   => MergeStrategy.discard
      case PathList(path @ _*) if path.last == "module-info.class" => MergeStrategy.discard
      case "reference.conf" | "application.conf"               => MergeStrategy.concat
      case _                                                   => MergeStrategy.deduplicate
    }
  )
