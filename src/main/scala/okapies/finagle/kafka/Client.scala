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
            offset: Long): Future[FetchResult] =
    fetch(topicName, partition, offset, Int.MaxValue)

  def fetch(topicName: String,
            partition: Int,
            offset: Long,
            maxBytes: Int): Future[FetchResult] =
    fetch(Map(topicName -> Map(partition -> FetchOffset(offset, maxBytes)))).map {
      _.results(topicName)(partition)
    }

  def fetch(partitions: Map[String, Map[Int, FetchOffset]],
            minBytes: Int = 0,
            maxWaitTime: Duration = Duration.Top,
            replicaId: Int = -1): Future[FetchResponse] =
    doRequest(FetchRequest(
        nextCorrelationId(),
        clientId,
        replicaId,
        maxWaitTime.inMilliseconds.toInt,
        minBytes,
        partitions)) {
      case res: FetchResponse => Future.value(res)
//      case res: StreamFetchResponse => Future.value(res)
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

  def offsetCommit(consumerGroup: String,
                   topicName: String,
                   partition: Int,
                   offset: Long,
                   metadata: String = ""): Future[OffsetCommitResult] =
    offsetCommit(consumerGroup, Map(topicName -> Map(partition ->
      CommitOffset(offset, metadata)))).map(_.values.head.values.head)

  def offsetCommit(consumerGroup: String,
                   commits: Map[String, Map[Int, CommitOffset]]
                   ): Future[Map[String, Map[Int, OffsetCommitResult]]] =
    doRequest(OffsetCommitRequest(
      nextCorrelationId(),
      clientId,
      consumerGroup,
      commits)) {
      case OffsetCommitResponse(_, results) => Future.value(results)
    }

  def offsetFetch(consumerGroup: String,
                  topicName: String,
                  partition: Int): Future[OffsetFetchResult] =
    offsetFetch(consumerGroup, Map(topicName -> Seq(partition)))
      .map(_.values.head.values.head)

  def offsetFetch(consumerGroup: String,
                  partitions: Map[String, Seq[Int]]
                  ): Future[Map[String, Map[Int, OffsetFetchResult]]] =
    doRequest(OffsetFetchRequest(
      nextCorrelationId(),
      clientId,
      consumerGroup,
      partitions)) {
      case OffsetFetchResponse(_, results) => Future.value(results)
    }

  def metadata(): Future[Seq[TopicMetadata]] = metadata(Array.empty[String]:_*)

  def metadata(topicNames: String*): Future[Seq[TopicMetadata]] =
    doRequest(MetadataRequest(
        nextCorrelationId(),
        clientId,
        topicNames)) {
      case MetadataResponse(_, _, topics) => Future.value(topics)
    }

  def consumerMetadata(consumerGroup: String): Future[ConsumerMetadataResult] =
    doRequest(ConsumerMetadataRequest(
        nextCorrelationId(),
        clientId,
        consumerGroup)) {
      case ConsumerMetadataResponse(_, result) => Future.value(result)
    }

  def close(deadline: Time): Future[Unit] = service.close(deadline)

  private[this] def doRequest[T](req: Request)(
    handler: PartialFunction[Response, Future[T]]) = service(req) flatMap handler

}
