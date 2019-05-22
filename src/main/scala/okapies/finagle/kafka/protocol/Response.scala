package okapies.finagle.kafka.protocol

import io.netty.buffer.ByteBuf

import com.twitter.concurrent.Offer
import com.twitter.util.Future

sealed trait Response {
  def correlationId: Int // int32
}

// ProduceResponse
case class ProduceResponse(
  correlationId: Int, // int32
  results: Map[String, Map[Int, ProduceResult]]
    // [TopicName [Partition ErrorCode Offset]]
) extends Response

case class ProduceResult(
  error: KafkaError, // int16
  offset: Long       // int64
)

// FetchResponse
case class FetchResponse(
  correlationId: Int, // int32
  results: Map[String, Map[Int, FetchResult]]
    // [TopicName [Partition ErrorCode HighwaterMarkOffset MessageSetSize MessageSet]]
) extends Response

case class FetchResult(
  error: KafkaError,               // int16
  highwaterMarkOffset: Long,       // int64
  messages: Seq[MessageWithOffset] // MessageSetSize MessageSet
)

case class StreamFetchResponse(
  correlationId: Int, // int32
  partitions: Offer[PartitionStatus],
  messages: Offer[FetchedMessage],
  onComplete: Future[Unit]
) extends Response

// OffsetResponse
case class OffsetResponse(
  correlationId: Int,        // int32
  results: Map[String, Map[Int, OffsetResult]]
    // [TopicName [PartitionOffsets]]
) extends Response

case class OffsetResult(
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
  results: Map[String, Map[Int, OffsetCommitResult]]
) extends Response

case class OffsetCommitResult(
  error: KafkaError // int16
)

// OffsetFetchResponse
case class OffsetFetchResponse(
  correlationId: Int, // int32
  results: Map[String, Map[Int, OffsetFetchResult]]
) extends Response

case class OffsetFetchResult(
  offset: Long,      // int64
  metadata: String,  // string
  error: KafkaError  // int16
)

// ConsumerMetadataResponse
case class ConsumerMetadataResponse(
  correlationId: Int, // int32
  result: ConsumerMetadataResult
) extends Response

case class ConsumerMetadataResult(
  error: KafkaError,  // int16
  id: Int,            // int32
  host: String,       // string
  port: Int           // int32
)

// NilResponse, used by server to indicate that no response is needed
case class NilResponse(correlationId:Int) extends Response

/**
 * A message frame for responses.
 */
sealed trait ResponseFrame

case class BufferResponseFrame(
  apiKey: Short,
  correlationId: Int,
  frame: ByteBuf
) extends ResponseFrame

case class FetchResponseFrame(
  correlationId: Int
) extends ResponseFrame

case class PartitionStatus(
  topicName: String,        // string
  partition: Int,           // int32
  error: KafkaError,        // int16
  highwaterMarkOffset: Long // int64
) extends ResponseFrame

case class FetchedMessage(
  topicName: String, // string
  partition: Int,    // int32
  offset: Long,      // int64
  message: Message
) extends ResponseFrame

case object NilMessageFrame extends ResponseFrame
