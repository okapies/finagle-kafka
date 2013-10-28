package okapies.finagle.kafka

import java.util.concurrent.atomic.AtomicInteger

import com.twitter.finagle.Service
import com.twitter.util.{Time, Closable, Duration, Future}

import okapies.finagle.kafka.protocol.{Request, Response}

object Client {

  def apply(raw: Service[Request, Response]): Client = new Client(raw)

}

class Client(
  service: Service[Request, Response],
  clientId: String = ""
) extends Closable {

  import okapies.finagle.kafka.protocol._

  private[this] val correlationId = new AtomicInteger(0)

  private[this] def nextCorrelationId(): Int = correlationId.incrementAndGet()

  def produce(topicName: String, partition: Int, messageSet: Message*): Future[ProduceResult] =
    produce(Map(topicName -> Map(partition -> messageSet))).map(_.values.head.values.head)

  def produce(messageSets: Map[String, Map[Int, Seq[Message]]],
              timeout: Duration = Duration.Top,
              requiredAcks: RequiredAcks = RequiredAcks.WaitForLeader
               ): Future[Map[String, Map[Int, ProduceResult]]] =
    doRequest(ProduceRequest(
        nextCorrelationId(),
        clientId,
        requiredAcks,
        timeout.inMilliseconds.toInt,
        messageSets)) {
      case ProduceResponse(_, results) => Future.value(results)
    }

  def fetch(topicName: String,
            partition: Int,
            offset: Long): Future[StreamFetchResponse] =
    fetch(topicName, partition, offset, Int.MaxValue)

  def fetch(topicName: String,
            partition: Int,
            offset: Long,
            maxBytes: Int): Future[StreamFetchResponse] =
    fetch(Map(topicName -> Map(partition -> FetchOffset(offset, maxBytes))))

  def fetch(partitions: Map[String, Map[Int, FetchOffset]],
            minBytes: Int = 0,
            maxWaitTime: Duration = Duration.Top,
            replicaId: Int = -1): Future[StreamFetchResponse] =
    doRequest(FetchRequest(
        nextCorrelationId(),
        clientId,
        replicaId,
        maxWaitTime.inMilliseconds.toInt,
        minBytes,
        partitions)) {
      case res: StreamFetchResponse => Future.value(res)
    }

  def offset(topicName: String, partition: Int): Future[OffsetResult] =
    offset(topicName, partition, -1)

  def offset(topicName: String, partition: Int, time: Long): Future[OffsetResult] =
    offset(topicName, partition, time, Int.MaxValue)

  def offset(topicName: String,
             partition: Int,
             time: Long,
             maxNumberOfOffsets: Int): Future[OffsetResult] =
    offset(Map(topicName -> Map(partition ->
      OffsetFilter(time, maxNumberOfOffsets)))).map(_.values.head.values.head)

  def offset(partitions: Map[String, Map[Int, OffsetFilter]],
             replicaId: Int = -1): Future[Map[String, Map[Int, OffsetResult]]] =
    doRequest(OffsetRequest(
      nextCorrelationId(),
      clientId,
      replicaId,
      partitions)) {
      case OffsetResponse(_, results) => Future.value(results)
    }

  def metadata(): Future[Seq[TopicMetadata]] = metadata(Array.empty[String]:_*)

  def metadata(topicNames: String*): Future[Seq[TopicMetadata]] =
    doRequest(MetadataRequest(
        nextCorrelationId(),
        clientId,
        topicNames)) {
      case MetadataResponse(_, _, topics) => Future.value(topics)
    }

  def close(deadline: Time): Future[Unit] = service.close(deadline)

  private[this] def doRequest[T](req: Request)(
    handler: PartialFunction[Response, Future[T]]) = service(req) flatMap handler

}
