name := "finagle-kafka"

description := "An Apache Kafka client in Netty and Finagle."

organization := "com.github.okapies"

organizationHomepage := Some(url("https://github.com/okapies"))

version := "0.2.3-SNAPSHOT"

scalaVersion := "2.11.12"

crossScalaVersions := Seq("2.11.12")

libraryDependencies ++= List(
  "com.twitter" %% "finagle-core" % "19.5.1",
  "com.twitter" %% "finagle-netty4" % "19.5.1",
  "org.apache.kafka" %% "kafka" % "0.8.2.1"
    exclude("com.101tec", "zkclient")
    exclude("com.yammer.metrics", "metrics-core")
    exclude("net.sf.jopt-simple", "jopt-simple")
    exclude("org.apache.zookeeper", "zookeeper")
    exclude("org.xerial.snappy", "snappy-java"),
  "org.scalatest" %% "scalatest" % "3.0.5" % "test",
  // dependencies for kafka-test
  "junit" % "junit" % "4.12" % "test",
  "org.apache.curator" % "curator-test" % "4.1.0" % "test",
  "com.101tec" % "zkclient" % "0.11" % "test",
  "com.yammer.metrics" % "metrics-core" % "2.2.0" % "test",
  "org.apache.kafka" %% "kafka" % "0.8.2.1" % "test" classifier "test"
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
