package okapies.finagle.kafka.protocol

import org.jboss.netty.buffer.ChannelBuffer
import org.jboss.netty.channel.{Channel, ChannelHandlerContext}
import org.jboss.netty.handler.codec.oneone.{OneToOneEncoder, OneToOneDecoder}

class ResponseDecoder(selector: Int => Option[Short]) extends OneToOneDecoder {

  import Spec._

  def decode(ctx: ChannelHandlerContext, channel: Channel, msg: AnyRef) = msg match {
    case buf: ChannelBuffer =>
      // Note: MessageSize field must be discarded by previous frame decoder.
      val correlationId = buf.decodeInt32()
      selector(correlationId).map { apiKey =>
        apiKey match {
          case ApiKeyProduce => decodeProduceResponse(correlationId, buf)
          case ApiKeyFetch => decodeFetchResponse(correlationId, buf)
          case ApiKeyOffset => decodeOffsetResponse(correlationId, buf)
          case ApiKeyMetadata => decodeMetadataResponse(correlationId, buf)
          case ApiKeyLeaderAndIsr => msg
          case ApiKeyStopReplica => msg
          case ApiKeyOffsetCommit => decodeOffsetCommitResponse(correlationId, buf)
          case ApiKeyOffsetFetch => decodeOffsetFetchResponse(correlationId, buf)
        }
      }.getOrElse(msg)
    case _ => msg // fall through
  }

  /**
   * {{{
   * ProduceResponse => [TopicName [Partition ErrorCode Offset]]
   * }}}
   */
  private def decodeProduceResponse(correlationId: Int, buf: ChannelBuffer) = {
    val results = buf.decodeArray {
      val topicName = buf.decodeString()

      // [Partition ErrorCode Offset]
      buf.decodeArray {
        val partition = buf.decodeInt32()
        val error: KafkaError = buf.decodeInt16()
        val offset = buf.decodeInt64()

        ProduceResult(TopicPartition(topicName, partition), error, offset)
      }
    }.flatten

    ProduceResponse(correlationId, results)
  }

  /**
   * {{{
   * FetchResponse => [TopicName [Partition ErrorCode HighwaterMarkOffset MessageSetSize MessageSet]]
   * }}}
   */
  private def decodeFetchResponse(correlationId: Int, buf: ChannelBuffer) = {
    val results = buf.decodeArray {
      val topicName = buf.decodeString()

      // [Partition ErrorCode HighwaterMarkOffset MessageSetSize MessageSet]
      buf.decodeArray {
        val partition = buf.decodeInt32()
        val error: KafkaError = buf.decodeInt16()
        val highwaterMarkOffset = buf.decodeInt64()
        val messages = buf.decodeMessageSet()

        FetchResult(TopicPartition(topicName, partition), error, highwaterMarkOffset, messages)
      }
    }.flatten

    FetchResponse(correlationId, results)
  }

  /**
   * {{{
   * OffsetResponse => [TopicName [PartitionOffsets]]
   *   PartitionOffsets => Partition ErrorCode [Offset]
   * }}}
   */
  private def decodeOffsetResponse(correlationId: Int, buf: ChannelBuffer) = {
    val results = buf.decodeArray {
      val topicName = buf.decodeString()

      // [Partition ErrorCode [Offset]]
      buf.decodeArray {
        val partition = buf.decodeInt32()
        val error: KafkaError = buf.decodeInt16()
        val offsets = buf.decodeArray(buf.decodeInt64())

        OffsetResult(TopicPartition(topicName, partition), error, offsets)
      }
    }.flatten

    OffsetResponse(correlationId, results)
  }

  /**
   * {{{
   * MetadataResponse => [Broker][TopicMetadata]
   *   Broker => NodeId Host Port
   *   TopicMetadata => TopicErrorCode TopicName [PartitionMetadata]
   *     PartitionMetadata => PartitionErrorCode PartitionId Leader Replicas Isr
   * }}}
   */
  private def decodeMetadataResponse(correlationId: Int, buf: ChannelBuffer) = {
    // [Broker]
    val brokers = buf.decodeArray {
      val nodeId = buf.decodeInt32()
      val host = buf.decodeString()
      val port = buf.decodeInt32()

      Broker(nodeId, host, port)
    }
    val toBroker = brokers.map(b => (b.nodeId, b)).toMap // nodeId to Broker

    // [TopicMetadata]
    val topics = buf.decodeArray {
      val errorCode = buf.decodeInt16()
      val name = buf.decodeString()

      // [PartitionMetadata]
      val partitions = buf.decodeArray {
        val errorCode = buf.decodeInt16()
        val id = buf.decodeInt32()
        val leader = toBroker.get(buf.decodeInt32())
        val replicas = buf.decodeArray(toBroker(buf.decodeInt32()))
        val isr = buf.decodeArray(toBroker(buf.decodeInt32()))

        PartitionMetadata(errorCode, id, leader, replicas, isr)
      }

      TopicMetadata(errorCode, name, partitions)
    }

    MetadataResponse(correlationId, brokers, topics)
  }

  /**
   * Not implemented in Kafka 0.8. See KAFKA-993
   *
   * {{{
   * OffsetCommitResponse => ClientId [TopicName [Partition ErrorCode]]]
   * }}}
   */
  private def decodeOffsetCommitResponse(correlationId: Int, buf: ChannelBuffer) = {
    val clientId = buf.decodeString()
    val results = buf.decodeArray {
      val topicName = buf.decodeString()

      // [Partition ErrorCode [Offset]]
      buf.decodeArray {
        val partition = buf.decodeInt32()
        val error: KafkaError = buf.decodeInt16()

        OffsetCommitResult(TopicPartition(topicName, partition), error)
      }
    }.flatten

    OffsetCommitResponse(correlationId, clientId, results)
  }

  /**
   * Not implemented in Kafka 0.8. See KAFKA-993
   *
   * {{{
   * OffsetFetchResponse => ClientId [TopicName [Partition Offset Metadata ErrorCode]]
   * }}}
   */
  private def decodeOffsetFetchResponse(correlationId: Int, buf: ChannelBuffer) = {
    val clientId = buf.decodeString()
    val results = buf.decodeArray {
      val topicName = buf.decodeString()

      // [Partition ErrorCode [Offset]]
      buf.decodeArray {
        val partition = buf.decodeInt32()
        val offset = buf.decodeInt64()
        val metadata = buf.decodeString()
        val error: KafkaError = buf.decodeInt16()

        OffsetFetchResult(TopicPartition(topicName, partition), offset, metadata, error)
      }
    }.flatten

    OffsetFetchResponse(correlationId, clientId, results)
  }

}

/**
 *  TODO: Not implemented yet.
 */
class ResponseEncoder extends OneToOneEncoder {

  import Spec._

  def encode(ctx: ChannelHandlerContext, channel: Channel, msg: AnyRef) = msg

}
