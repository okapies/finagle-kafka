package okapies.finagle.kafka

import org.scalatest._
import org.scalatest.matchers._
import io.netty.buffer.Unpooled
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
  val Msg = Message.create(Unpooled.EMPTY_BUFFER)
  val ConsumerGroup = "test-group"

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

  val offsetResp = OffsetResponse(0, Map(Topic ->
    Map(0 -> OffsetResult(KafkaError.NoError, Seq(0,1)))))

  val fetchResp = FetchResponse(0, Map(Topic ->
    Map(0 -> FetchResult(KafkaError.NoError, 0, Seq(MessageWithOffset(0, Msg))))))

  val offsetCommitResp = OffsetCommitResponse(0, Map(Topic ->
    Map(0 -> OffsetCommitResult(KafkaError.NoError))))

  val offsetFetchResp = OffsetFetchResponse(0, Map(Topic ->
    Map(0 -> OffsetFetchResult(0, "metadata", KafkaError.NoError))))

  val consumerMetadataResp = ConsumerMetadataResponse(0,
    ConsumerMetadataResult(KafkaError.NoError, 0, "broker", 9092))

  // service that responds corresponding to the request
  val service = Service.mk[Request, Response] {
    case req: MetadataRequest =>
      Future.value(metadataResp)

    case req: FetchRequest =>
      Future.value(fetchResp)

    case req: ProduceRequest =>
      Future.value(produceResp)

    case req: OffsetRequest =>
      Future.value(offsetResp)

    case req: OffsetCommitRequest =>
      Future.value(offsetCommitResp)

    case req: OffsetFetchRequest =>
      Future.value(offsetFetchResp)

    case req: ConsumerMetadataRequest =>
      Future.value(consumerMetadataResp)

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
    client = Kafka.client.newService(addr)
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

    it should "respond to fetch requests" in {
      val resp = result(client(FetchRequest(0, ClientId, 0, 0, 0,
        Map(Topic -> Map(0 -> FetchOffset(0, 1))))))

      resp should be(fetchResp)
    }

    it should "respond to offset requests" in {
      val resp = result(client(OffsetRequest(0, ClientId, 0,
        Map(Topic -> Map(0 -> OffsetFilter(0, 1))))))

      resp should be(offsetResp)
    }

    it should "respond to metadata requests" in {
      val resp = result(client(MetadataRequest(0, ClientId, Seq(Topic))))

      resp should be(metadataResp)
    }

    it should "respond to offset commit requests (v1)" in {
      val resp = result(client(OffsetCommitRequest(0, ClientId, ConsumerGroup,
        Map(Topic -> Map(0 -> CommitOffset(0, "metadata"))))))

      resp should be(offsetCommitResp)
    }

    it should "respond to offset fetch requests" in {
      val resp = result(client(OffsetFetchRequest(0, ClientId, ConsumerGroup,
        Map(Topic -> Seq(0)))))

      resp should be(offsetFetchResp)
    }

    it should "respond to consumer metadata requests" in {
      val resp = result(client(ConsumerMetadataRequest(0, ClientId, ConsumerGroup)))

      resp should be(consumerMetadataResp)
    }

}
