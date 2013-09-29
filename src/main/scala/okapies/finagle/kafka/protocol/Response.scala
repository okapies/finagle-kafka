package okapies.finagle.kafka.protocol

sealed trait Response {
  def correlationId: Int // int32
}

// ProduceResponse
case class ProduceResponse(
  correlationId: Int,         // int32
  results: Seq[ProduceResult] // [TopicName [Partition ErrorCode Offset]]
) extends Response

case class ProduceResult(
  topicPartition: TopicPartition,
  error: KafkaError, // int16
  offset: Long       // int64
)

// FetchResponse
case class FetchResponse(
  correlationId: Int, // int32
  results: Seq[FetchResult]
    // [TopicName [Partition ErrorCode HighwaterMarkOffset MessageSetSize MessageSet]]
) extends Response

case class FetchResult(
  topicPartition: TopicPartition,
  error: KafkaError,               // int16
  highwaterMarkOffset: Long,       // int64
  messages: Seq[MessageWithOffset] // MessageSetSize MessageSet
)

// OffsetResponse
case class OffsetResponse(
  correlationId: Int,        // int32
  results: Seq[OffsetResult] // [TopicName [PartitionOffsets]]
) extends Response

case class OffsetResult(
  topicPartition: TopicPartition,
  error: KafkaError, // int16
  offsets: Seq[Long] // [int64]
)

// MetadataResponse
case class MetadataResponse(
  correlationId: Int,        // int32
  brokers: Seq[Broker],      // [Broker]
  topics: Seq[TopicMetadata] // [TopicMetadata]
) extends Response

case class Broker(
  nodeId: Int,  // int32
  host: String, // string
  port: Int     // int32
)

case class TopicMetadata(
  error: KafkaError,                 // int16
  name: String,                      // string
  partitions: Seq[PartitionMetadata] // [PartitionMetadata]
)

case class PartitionMetadata(
  error: KafkaError,      // int16
  id: Int,                // int32
  leader: Option[Broker], // int32
  replicas: Seq[Broker],  // [int32]
  isr: Seq[Broker]        // [int32]
)

// OffsetCommitReponse
case class OffsetCommitResponse(
  correlationId: Int, // int32
  clientId: String,   // string
  results: Seq[OffsetCommitResult]
) extends Response

case class OffsetCommitResult(
  topicPartition: TopicPartition,
  error: KafkaError // int16
)

// OffsetFetchResponse
case class OffsetFetchResponse(
  correlationId: Int, // int32
  clientId: String,   // string
  results: Seq[OffsetFetchResult]
) extends Response

case class OffsetFetchResult(
  topicPartition: TopicPartition,
  offset: Long,      // int64
  metadata: String,  // string
  error: KafkaError  // int16
)
