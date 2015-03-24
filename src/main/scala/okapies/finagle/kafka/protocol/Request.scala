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
    classOf[OffsetFetchRequest] -> ApiKeyOffsetFetch,
    classOf[ConsumerMetadataRequest] -> ApiKeyConsumerMetadata
  )

}

// ProduceRequest
case class ProduceRequest(
  correlationId: Int,         // int32
  clientId: String,           // string
  requiredAcks: RequiredAcks, // int16
  timeout: Int,               // int32 (msecs)
  messageSets: Map[String, Map[Int, Seq[Message]]]
    // [TopicName [Partition MessageSetSize MessageSet]]
) extends Request

// FetchRequest
case class FetchRequest(
  correlationId: Int, // int32
  clientId: String,   // string
  replicaId: Int,     // int32
  maxWaitTime: Int,   // int32 (msecs)
  minBytes: Int,      // int32
  topicPartitions: Map[String, Map[Int, FetchOffset]]
    // [TopicName [Partition FetchOffset MaxBytes]
) extends Request

case class FetchOffset(
  offset: Long, // int64
  maxBytes: Int // int32
)

// OffsetRequest
case class OffsetRequest(
  correlationId: Int, // int32
  clientId: String,   // string
  replicaId: Int,     // int32
  topicPartitions: Map[String, Map[Int, OffsetFilter]]
    // [TopicName [Partition Time MaxNumberOfOffsets]]
) extends Request

case class OffsetFilter(
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
  commits: Map[String, Map[Int, CommitOffset]]
) extends Request

case class CommitOffset(
  offset: Long,    // int64
  metadata: String // string
)

// OffsetFetchRequest
case class OffsetFetchRequest(
  correlationId: Int,    // int32
  clientId: String,      // string
  consumerGroup: String, // string
  partitions: Map[String, Seq[Int]]
) extends Request

// ConsumerMetadataRequest
case class ConsumerMetadataRequest(
  correlationId: Int,   // int32
  clientId: String,     // string
  consumerGroup: String // string
) extends Request
