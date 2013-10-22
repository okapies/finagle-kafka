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

  def produce(
      topicName: String,
      partitionId: Int,
      messages: Message*): Future[Seq[ProduceResult]] =
    produce(Seq(ProduceRequestPartition(TopicPartition(topicName, partitionId), messages)))

  def produce(
      partitions: Seq[ProduceRequestPartition],
      timeout: Duration = Duration.Top,
      requiredAcks: RequiredAcks = WaitLeader): Future[Seq[ProduceResult]] =
    doRequest(ProduceRequest(
        nextCorrelationId(),
        clientId,
        requiredAcks.acks,
        timeout.inMilliseconds.toInt,
        partitions)) {
      case ProduceResponse(_, results) => Future.value(results)
    }

  def fetch(
      topicName: String,
      partitionId: Int,
      offset: Long): Future[StreamFetchResponse] =
    fetch(topicName, partitionId, offset, Int.MaxValue)

  def fetch(
      topicName: String,
      partitionId: Int,
      offset: Long,
      maxBytes: Int): Future[StreamFetchResponse] =
    fetch(Seq(FetchRequestPartition(TopicPartition(topicName, partitionId), offset, maxBytes)))

  def fetch(
      partitions: Seq[FetchRequestPartition],
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

  def offset(topicName: String, partitionId: Int): Future[Seq[OffsetResult]] =
    offset(topicName, partitionId, -1)

  def offset(topicName: String, partitionId: Int, time: Long): Future[Seq[OffsetResult]] =
    offset(topicName, partitionId, time, Int.MaxValue)

  def offset(
      topicName: String,
      partitionId: Int,
      time: Long,
      maxNumberOfOffsets: Int): Future[Seq[OffsetResult]] =
    offset(Seq(OffsetRequestPartition(
      TopicPartition(topicName, partitionId), time, maxNumberOfOffsets)))

  def offset(
      partitions: Seq[OffsetRequestPartition],
      replicaId: Int = -1): Future[Seq[OffsetResult]] =
    doRequest(OffsetRequest(
      nextCorrelationId(),
      clientId,
      replicaId,
      partitions)) {
      case OffsetResponse(_, results) => Future.value(results)
    }

  def metadata(): Future[Seq[TopicMetadata]] = metadata(Array.empty:_*)

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
