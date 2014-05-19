name := "finagle-kafka"

description := "An Apache Kafka client in Netty and Finagle."

organization := "com.github.okapies"

organizationHomepage := Some(url("https://github.com/okapies"))

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.10.4"

net.virtualvoid.sbt.graph.Plugin.graphSettings

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

publishTo := {
  val nexus = "https://oss.sonatype.org/"
  if (version.value.trim.endsWith("SNAPSHOT"))
    Some("snapshots" at nexus + "content/repositories/snapshots")
  else
    Some("releases"  at nexus + "service/local/staging/deploy/maven2")
}

publishMavenStyle := true

publishArtifact in Test := false

pomIncludeRepository := { _ => false }

pomExtra := (
  <url>https://github.com/okapies/finagle-kafka</url>
    <licenses>
      <license>
        <name>The MIT License</name>
        <url>http://www.opensource.org/licenses/mit-license.php</url>
        <distribution>repo</distribution>
      </license>
    </licenses>
    <scm>
      <url>git@github.com:okapies/finagle-kafka.git</url>
      <connection>scm:git:git@github.com:okapies/finagle-kafka.git</connection>
    </scm>
    <developers>
      <developer>
        <id>okapies</id>
        <name>Yuta Okamoto</name>
        <url>https://github.com/okapies</url>
      </developer>
    </developers>)
