package okapies.finagle.kafka.protocol

sealed trait Request {
  def correlationId: Int // int32
  def clientId: String   // string
}

object Request {

  import Spec._

  def toApiKey(req: Request) = toApiKeyMap(req.getClass)

  private[this] val toApiKeyMap = Map[Class[_ <: Request], Short](
    classOf[ProduceRequest] -> ApiKeyProduce,
    classOf[FetchRequest] -> ApiKeyFetch,
    classOf[OffsetRequest] -> ApiKeyOffset,
    classOf[MetadataRequest] -> ApiKeyMetadata,
    classOf[OffsetCommitRequest] -> ApiKeyOffsetCommit,
    classOf[OffsetFetchRequest] -> ApiKeyOffsetFetch
  )

}

// ProduceRequest
case class ProduceRequest(
  correlationId: Int,  // int32
  clientId: String,    // string
  requiredAcks: Short, // int16
  timeout: Int,        // int32 (msecs)
  partitions: Seq[ProduceRequestPartition] // [TopicName [Partition MessageSetSize MessageSet]]
) extends Request

case class ProduceRequestPartition(
  topicPartition: TopicPartition,
  messages: Seq[Message]
)

// FetchRequest
case class FetchRequest(
  correlationId: Int, // int32
  clientId: String,   // string
  replicaId: Int,     // int32
  maxWaitTime: Int,   // int32 (msecs)
  minBytes: Int,      // int32
  partitions: Seq[FetchRequestPartition] // [TopicName [Partition FetchOffset MaxBytes]
) extends Request

case class FetchRequestPartition(
  topicPartition: TopicPartition,
  offset: Long, // int64
  maxBytes: Int // int32
)

// OffsetRequest
case class OffsetRequest(
  correlationId: Int, // int32
  clientId: String,   // string
  replicaId: Int,     // int32
  partitions: Seq[OffsetRequestPartition] // [TopicName [Partition Time MaxNumberOfOffsets]]
) extends Request

case class OffsetRequestPartition(
  topicPartition: TopicPartition,
  time: Long,             // int64 (msecs)
  maxNumberOfOffsets: Int // int32
)

// MetadataRequest
case class MetadataRequest(
  correlationId: Int, // int32
  clientId: String,   // string
  topics: Seq[String] // [string]
) extends Request

// OffsetCommitRequest
case class OffsetCommitRequest(
  correlationId: Int,    // int32
  clientId: String,      // string
  consumerGroup: String, // string
  partitions: Seq[OffsetCommitRequestPartition]
) extends Request

case class OffsetCommitRequestPartition(
  topicPartition: TopicPartition,
  offset: Long,    // int64
  metadata: String // string
)

// OffsetFetchRequest
case class OffsetFetchRequest(
  correlationId: Int,    // int32
  clientId: String,      // string
  consumerGroup: String, // string
  partitions: Seq[TopicPartition]
) extends Request
