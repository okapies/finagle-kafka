name := "finagle-kafka"

version := "0.1.0-SNAPSHOT"

scalaVersion := "2.10.2"

net.virtualvoid.sbt.graph.Plugin.graphSettings

libraryDependencies ++= List(
  "com.twitter" % "finagle-core_2.10" % "6.11.1",
  "org.apache.kafka" % "kafka_2.10" % "0.8.0"
    exclude("com.sun.jmx", "jmxri")
    exclude("com.sun.jdmk", "jmxtools")
    exclude("log4j", "log4j")
    exclude("jline", "jline"),
  "org.scalatest" % "scalatest_2.10" % "2.0" % "test",
  "log4j" % "log4j" % "1.2.15"
    exclude("com.sun.jmx", "jmxri")
    exclude("com.sun.jdmk", "jmxtools")
    exclude("javax.jms", "jms")
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
  <url>http://jsuereth.com/scala-arm</url>
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
