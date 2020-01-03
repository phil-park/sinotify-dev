name := "sinotify"

version := "0.1"

scalaVersion := "2.11.8"

libraryDependencies ++= Seq(
  "org.apache.hadoop" % "hadoop-common" % "3.2.1",
  "org.apache.hadoop" % "hadoop-hdfs" % "3.2.1",
  "org.apache.hadoop" % "hadoop-client" % "3.2.1"
)

scalacOptions ++= Seq(
  "-encoding", "UTF-8", "-optimise",
  "-deprecation", "-unchecked", "-feature", "-Xlint",
  "-Ywarn-infer-any"
)

javacOptions ++= Seq(
  "-Xlint:unchecked", "-Xlint:deprecation"
)

