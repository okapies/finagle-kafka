name := "finagle-kafka"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.9.2"

net.virtualvoid.sbt.graph.Plugin.graphSettings

resolvers ++= List(
  "Apache repo" at "https://repository.apache.org/content/repositories/releases",
  "Twitter Maven repo" at "http://maven.twttr.com/",
  "conjars.org" at "http://conjars.org/repo"
)

libraryDependencies ++= List(
  "com.twitter" % "finagle-core_2.9.2" % "6.6.0",
  "org.apache.kafka" % "kafka_2.9.2" % "0.8.0-beta1"
    exclude("com.sun.jmx", "jmxri")
    exclude("com.sun.jdmk", "jmxtools")
    exclude("log4j", "log4j")
    exclude("jline", "jline"),
  "org.scalatest" % "scalatest_2.9.2" % "1.9.2" % "test",
  "log4j" % "log4j" % "1.2.15" % "test"
    exclude("com.sun.jmx", "jmxri")
    exclude("com.sun.jdmk", "jmxtools")
    exclude("javax.jms", "jms")
)
