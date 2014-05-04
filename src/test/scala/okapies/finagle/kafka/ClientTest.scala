package okapies.finagle.kafka 
package test

import org.scalatest._
import org.scalatest.matchers._

import java.util.Properties
import java.nio.charset.Charset

import com.twitter.util.Await
import kafka.admin.AdminUtils
import kafka.log.LogConfig
import kafka.utils.ZKStringSerializer
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.I0Itec.zkclient.ZkClient
import okapies.finagle.Kafka
import okapies.finagle.kafka.protocol.{KafkaError, Message}

object TestUtils {
  val UTF_8 = Charset.forName("UTF-8")

  implicit class CBString(s:String) {
    def cb = ChannelBuffers.copiedBuffer(s, UTF_8)
  }
}

class ClientTest
extends FlatSpec
with Matchers
with BeforeAndAfterAll {
  import TestUtils._
  import Await.result
  import KafkaError._

  val client = Kafka.newRichClient("localhost:9092")
  val topic = "finagle.kafka.test.topic"
  val group = "test-group"
  val msg = Message.create("hello".cb)

  def withZk(f:ZkClient => Unit) {
    val zk = new ZkClient("localhost:2181", 5000, 5000, ZKStringSerializer)
    f(zk)
    zk.close
  }

  override def beforeAll {
    withZk { zk =>
      AdminUtils.deleteTopic(zk, topic)
      AdminUtils.createTopic(zk, topic, 1, 1, new Properties)
    }
  }

  override def afterAll {
    withZk { zk =>
      AdminUtils.deleteTopic(zk, topic)
    }
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
    fetch.error should equal(NoError)
    fetch.metadata should equal(metadata)
  }
  */
}

