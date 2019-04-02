package okapies.finagle.kafka 

import org.scalatest._
import org.scalatest.matchers._
import org.scalatest.concurrent.Eventually
import org.scalatest.time.{Span, Seconds, Millis}

import java.util.Properties
import java.nio.charset.Charset

import com.twitter.util.Await
import _root_.kafka.admin.AdminUtils
import _root_.kafka.utils.{Utils, TestUtils, ZKStringSerializer}
import _root_.kafka.server.{KafkaConfig, KafkaServer}
import org.apache.curator.test.TestingServer
import io.netty.buffer.Unpooled
import org.I0Itec.zkclient.ZkClient

import okapies.finagle.Kafka
import okapies.finagle.kafka.protocol.{KafkaError, Message}

object ClientTestUtils {
  val UTF_8 = Charset.forName("UTF-8")

  implicit class ByteBufString(s:String) {
    def byteBuf = Unpooled.copiedBuffer(s, UTF_8)
  }
}

trait KafkaTest extends BeforeAndAfterAll { suite: Suite =>

  var zkServer: TestingServer = _
  var zkClient: ZkClient = _
  var kafkaServer: KafkaServer = _
  var kafkaConn: String = _
  var kafkaConfig: Properties = _

  override def beforeAll {
    zkServer = new TestingServer()

    val zkConn = zkServer.getConnectString

    kafkaConfig = TestUtils.createBrokerConfig(1)
    kafkaConfig.put("zookeeper.connect", zkConn)
    kafkaConfig.put("host.name", "127.0.0.1")

    // https://github.com/apache/kafka/blob/0.8.2/core/src/test/scala/unit/
    // kafka/server/OffsetCommitTest.scala#L49
    kafkaConfig.put("offsets.topic.replication.factor", "1")
    kafkaConn = s"""${kafkaConfig.get("host.name")}:${kafkaConfig.get("port")}"""
    kafkaServer = TestUtils.createServer(new KafkaConfig(kafkaConfig))

    zkClient = new ZkClient(zkConn, 5000, 5000, ZKStringSerializer)
  }

  override def afterAll {
    kafkaServer.shutdown()
    Utils.rm(kafkaConfig.getProperty("log.dir"))
    zkClient.close()
    zkServer.stop()
    Utils.rm(zkServer.getTempDirectory)
  }

}

class ClientTest
extends FlatSpec
with Matchers
with Eventually
with KafkaTest {
  import ClientTestUtils._
  import Await.result
  import KafkaError._

  var client:Client = _
  val topic = "finagle.kafka.test.topic"
  val group = "test-group"
  val msg = Message.create("hello".byteBuf)

  override implicit val patienceConfig =
    PatienceConfig(timeout = scaled(Span(5, Seconds)), interval = scaled(Span(500, Millis)))


  override def beforeAll {
    // start zookeeper and kafka
    super.beforeAll

    // init the kafka client
    client = Kafka.newRichClient(kafkaConn)

    //  create the topic for testing
    AdminUtils.createTopic(zkClient, topic, 1, 1, new Properties)

    // Make sure the topic leader is available before running tests
    TestUtils.waitUntilLeaderIsElectedOrChanged(zkClient, topic, 0, 1000)
  }

  "A kafka client" should "return metadata" in {
    val metadata = result(client.metadata(topic))
    metadata should have length (1)

    val entry = metadata.head
    
    entry.error should equal (NoError)
    entry.name should equal (topic)
    entry.partitions should have length (1)

    val partition = entry.partitions.head

    partition.error should equal (NoError)
    partition.id should equal (0)
    partition.leader should be ('defined)
    partition.replicas should have length (1)
    partition.isr should have length (1)
  }

  it should "return an empty offset" in {
    val offset = result(client.offset(topic, 0))

    offset.error should equal (NoError)
    offset.offsets should have length (1)
    offset.offsets.head should equal (0)
  }

  it should "return no messages when empty" in {
    val resp = result(client.fetch(topic, 0, 0))

    resp.error should equal (NoError)
    resp.highwaterMarkOffset should equal (0)
    resp.messages should have length (0)
  }

  it should "handle producing a single message" in {
    val resp = result(client.produce(topic, 0, Seq(msg):_*))

    resp.error should equal (NoError)
    resp.offset should equal (0)
  }

  it should "return next offset" in {
    val resp = result(client.offset(topic, 0))

    resp.error should equal (NoError)
    resp.offsets should have length (2)
    resp.offsets should equal (Seq(1,0))

    val respSingle = result(client.offset(topic, 0, -1, 1))
    respSingle.error should equal (NoError)
    respSingle.offsets should have length (1)
    respSingle.offsets should equal (Seq(1))
  }

  it should "return a single message at the first offset" in {
    val resp = result(client.fetch(topic, 0, 0))

    resp.error should equal (NoError)
    resp.highwaterMarkOffset should equal (1)
    resp.messages should have length (1)

    val kafkaMsg = resp.messages.head
    kafkaMsg.offset should equal (0)
    kafkaMsg.message should equal (msg)
  }

  it should "handle offset commit" in {
    val resp = result(client.offsetCommit(group, topic, 0, 1))

    resp.error should equal (NoError)
  }

  it should "return the last offset commit" in {
    val resp = result(client.offsetFetch(group, topic, 0))

    resp.error should equal (NoError)
    resp.offset should equal (1)
    resp.metadata should equal ("")
  }

  /* Metadata is not implemented in 0.8.1

  it should "handle offset commits with metadata" in {
    val metadata = "my test metadata"
    val commit = result(client.offsetCommit(group, topic, 0, 1, metadata))
    commit.error should equal (NoError)

    val fetch = result(client.offsetFetch(group, topic, 0))
    fetch.error should equal (NoError)
    fetch.metadata should equal(metadata)
  }
  */

  it should "return consumer group metadata" in {
    eventually {
      val resp = result(client.consumerMetadata(group))

      resp.error should equal (NoError)
      resp.id should equal (1)
      resp.host should equal (kafkaConfig.get("host.name"))
      resp.port should equal (kafkaConfig.get("port").asInstanceOf[String].toInt)
    }
  }
}

