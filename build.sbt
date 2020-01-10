name := "sinotify"

version := "0.1"

scalaVersion := "2.12.10"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common" % "3.2.1",
  "org.apache.hadoop" % "hadoop-hdfs" % "3.2.1",
  "org.apache.hadoop" % "hadoop-client" % "3.2.1",
  "org.json4s" %% "json4s-native" % "3.7.0-M2",
  "ch.qos.logback" % "logback-classic" % "1.2.3",
  "com.typesafe.scala-logging" %% "scala-logging" % "3.9.2"
)

val json4sNative = "org.json4s" %% "json4s-native" % "{latestVersion}"

scalacOptions ++= Seq(
  "-encoding", "UTF-8", "-optimise",
  "-deprecation", "-unchecked", "-feature", "-Xlint",
  "-Ywarn-infer-any"
)

javacOptions ++= Seq(
  "-Xlint:unchecked", "-Xlint:deprecation"
)

