name := "finagle-kafka"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.10.4"

net.virtualvoid.sbt.graph.Plugin.graphSettings

resolvers ++= List(
  "Apache repo" at "https://repository.apache.org/content/repositories/releases",
  "Twitter Maven repo" at "http://maven.twttr.com/",
  "conjars.org" at "http://conjars.org/repo"
)

libraryDependencies ++= List(
  "com.twitter" % "finagle-core_2.10" % "6.15.0",
  "org.apache.kafka" % "kafka_2.10" % "0.8.1.1"
    exclude("com.sun.jmx", "jmxri")
    exclude("com.sun.jdmk", "jmxtools")
    exclude("log4j", "log4j")
    exclude("jline", "jline"),
  "org.scalatest" % "scalatest_2.10" % "2.1.5" % "test",
  "log4j" % "log4j" % "1.2.15"
    exclude("com.sun.jmx", "jmxri")
    exclude("com.sun.jdmk", "jmxtools")
    exclude("javax.jms", "jms"),
  "org.apache.curator" % "curator-test" % "2.4.2" % "test"
)
