package okapies.finagle.kafka

import org.scalatest._
import org.scalatest.matchers._
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.concurrent.AsyncQueue
import com.twitter.util.{Future, Await}
import com.twitter.finagle.Service
import com.twitter.finagle.transport.{Transport, QueueTransport}

import okapies.finagle.Kafka
import protocol._


class ServerTest
extends FlatSpec
with Matchers {
  import Await._

  val broker = Broker(0, "test", 9092)
  val Topic = "test-topic"

  val partitionMetadata = PartitionMetadata(
    KafkaError.NoError,
    0,
    leader = Some(broker),
    replicas = Seq(broker),
    isr = Seq(broker)
  )

  val topicMetadata = TopicMetadata(
    KafkaError.NoError,
    Topic,
    Seq(partitionMetadata)
  )

  val metadataResp = MetadataResponse(0, Seq(broker), Seq(topicMetadata))

  // service that responds corresponding to the request
  val service = Service.mk[Request, Response] {
    case MetadataRequest(corrId, clientId, topics) =>
      Future.value(metadataResp)

    case req => Future.value(null)
  }

  private val addr = ":9993"

  // tests the server dispatcher
  "A Kafka Server" should "start and shutdown" in {
    val server = Kafka.serve(addr, service)
    result(server.close()) should be(())
  }

  it should "respond to a metadata request" in {
    val server = Kafka.serve(addr, service)

    // connect the client to the server
    // execute a request with the client

    ready(server.close())
  }
}

