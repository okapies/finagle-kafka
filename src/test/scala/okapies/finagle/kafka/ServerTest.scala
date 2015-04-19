package okapies.finagle.kafka

import org.scalatest._
import org.scalatest.matchers._
import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import com.twitter.concurrent.AsyncQueue
import com.twitter.util.{Future, Await}
import com.twitter.finagle.{Service, ListeningServer}
import com.twitter.finagle.transport.{Transport, QueueTransport}

import okapies.finagle.Kafka
import protocol._


class ServerTest
extends FlatSpec
with Matchers {
  import Await._

  private val addr = ":9993"

  // service that responds corresponding to the request
  val service = Service.mk[Request, Response] {
    case req => Future.value(null)
  }


  // tests the server dispatcher
  "A Kafka Server" should "start and shutdown" in {
    val server = Kafka.serve(addr, service)
    result(server.close()) should be(())
  }

  it should "handle a client connecting and disconnecting" in {
    val server = Kafka.serve(addr, service)

    val client = Kafka.newRichClient(addr)

    result(client.close()) should be(())

    ready(server.close())
  }
}

class ServerRequestResponseTest
extends FlatSpec
with Matchers
with BeforeAndAfterEach {
  import Await._

  val ClientId = "test-client"
  val Topic = "test-topic"
  val Msg = Message.create(ChannelBuffers.EMPTY_BUFFER)

  val broker = Broker(0, "test", 9092)

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

  val produceResult = ProduceResult(KafkaError.NoError, 0)

  val produceResp = ProduceResponse(0, Map(Topic -> Map(0 -> produceResult)))

  // service that responds corresponding to the request
  val service = Service.mk[Request, Response] {
    case MetadataRequest(corrId, clientId, topics) =>
      println("Got the request! " + topics)
      Future.value(metadataResp)

    case ProduceRequest(corrId, clientId, acks, timeout, msgs) =>
      Future.value(produceResp)

    case req => Future.value(null)
  }

  var client: Service[Request, Response] = _
  var server: ListeningServer = _
  var rand = new scala.util.Random()

  private def newAddr:String = {
    s":${10000 + rand.nextInt(1000)}"
  }

  override def beforeEach = {
    val addr = newAddr
    server = Kafka.serve(addr, service)
    client = Kafka.newService(server)
  }

  override def afterEach = {
    ready(client.close())
    result(server.close())
  }

  "A kafka server receiving requests" should
    "respond to produce requests" in {
      val resp = result(client(ProduceRequest(0, ClientId, RequiredAcks(0), 1000,
        Map(Topic -> Map(0 -> Seq(Msg))))))

      resp should be(produceResp)
    }

    it should "respond to metadata requests" in {
      val resp = result(client(MetadataRequest(0, ClientId, Seq(Topic))))

      resp should be(metadataResp)
    }
}
