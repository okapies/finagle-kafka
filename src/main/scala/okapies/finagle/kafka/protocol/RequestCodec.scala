package okapies.finagle.kafka.protocol

import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.channel.{Channel, ChannelHandlerContext}
import org.jboss.netty.handler.codec.oneone.{OneToOneDecoder, OneToOneEncoder}

/**
 *  TODO: Not implemented yet.
 */
class RequestDecoder extends OneToOneDecoder {

  import Spec._

  def decode(ctx: ChannelHandlerContext, channel: Channel, msg: AnyRef) = msg match {
    case bytes: Array[Byte] => null
  }

}

class RequestEncoder(logger: RequestLogger) extends OneToOneEncoder {

  import Spec._

  def encode(ctx: ChannelHandlerContext, channel: Channel, msg: AnyRef) = {
    val req = msg match {
      case req: ProduceRequest => encodeProduceRequest(req)
      case req: FetchRequest => encodeFetchRequest(req)
      case req: OffsetRequest => encodeOffsetRequest(req)
      case req: MetadataRequest => encodeMetadataRequest(req)
      case req: OffsetCommitRequest => encodeOffsetCommitRequest(req)
      case req: OffsetFetchRequest => encodeOffsetFetchRequest(req)
      case _ => msg // fall through
    }

    msg match {
      case req: Request => logger.append(req) // TODO: handle network error
      case _ =>
    }

    req
  }

  private def encodeRequestHeader(buf: ChannelBuffer, apiKey: Short, req: Request) {
    buf.encodeInt16(apiKey)            // Apikey => int16
    buf.encodeInt16(ApiVersion0)       // ApiVersion => int16
    buf.encodeInt32(req.correlationId) // CorrelationId => int32
    buf.encodeString(req.clientId)     // ClientId => string
  }

  /**
   * {{{
   * ProduceRequest => RequiredAcks Timeout [TopicName [Partition MessageSetSize MessageSet]]
   *   MessageSet => [Offset MessageSize Message]
   * }}}
   */
  private def encodeProduceRequest(req: ProduceRequest) = {
    val buf = ChannelBuffers.dynamicBuffer() // TODO: estimatedLength
    encodeRequestHeader(buf, ApiKeyProduce, req)

    buf.encodeInt16(req.requiredAcks)
    buf.encodeInt32(req.timeout)

    // [TopicName [Partition MessageSetSize MessageSet]]
    val topics = req.partitions.groupBy(_.topicPartition.topic) // group by topicName
    buf.encodeArray(topics) { case (topicName, partitions) =>
      buf.encodeString(topicName)
      buf.encodeArray(partitions) { partition =>
        buf.encodeInt32(partition.topicPartition.partition)
        buf.encodeMessageSet(partition.messages) { message =>
          buf.encodeInt64(0L)                 // Offset (Note: It can be any value)
          buf.encodeBytes(message.underlying) // MessageSize + Message
        }
      }
    }

    buf
  }

  /**
   * {{{
   * FetchRequest => ReplicaId MaxWaitTime MinBytes [TopicName [Partition FetchOffset MaxBytes]]
   * }}}
   */
  private def encodeFetchRequest(req: FetchRequest) = {
    val buf = ChannelBuffers.dynamicBuffer() // TODO: estimatedLength
    encodeRequestHeader(buf, ApiKeyFetch, req)

    buf.encodeInt32(req.replicaId)
    buf.encodeInt32(req.maxWaitTime)
    buf.encodeInt32(req.minBytes)

    // [TopicName [Partition FetchOffset MaxBytes]
    val topics = req.partitions.groupBy(_.topicPartition.topic) // group by topicName
    buf.encodeArray(topics) { case (topicName, partitions) =>
      buf.encodeString(topicName)
      buf.encodeArray(partitions) { partition =>
        buf.encodeInt32(partition.topicPartition.partition)
        buf.encodeInt64(partition.offset)
        buf.encodeInt32(partition.maxBytes)
      }
    }

    buf
  }

  /**
   * {{{
   * OffsetRequest => ReplicaId [TopicName [Partition Time MaxNumberOfOffsets]]
   * }}}
   */
  private def encodeOffsetRequest(req: OffsetRequest) = {
    val buf = ChannelBuffers.dynamicBuffer() // TODO: estimatedLength
    encodeRequestHeader(buf, ApiKeyOffset, req)

    buf.encodeInt32(req.replicaId)
    val topics = req.partitions.groupBy(_.topicPartition.topic) // group by topicName
    buf.encodeArray(topics) { case (topicName, partitions) =>
      buf.encodeString(topicName)
      buf.encodeArray(partitions) { partition =>
        buf.encodeInt32(partition.topicPartition.partition)
        buf.encodeInt64(partition.time)
        buf.encodeInt32(partition.maxNumberOfOffsets)
      }
    }

    buf
  }

  /**
   * {{{
   * MetadataRequest => [TopicName]
   * }}}
   */
  private def encodeMetadataRequest(req: MetadataRequest) = {
    val buf = ChannelBuffers.dynamicBuffer() // TODO: estimatedLength
    encodeRequestHeader(buf, ApiKeyMetadata, req)

    buf.encodeStringArray(req.topics) // [TopicName]

    buf
  }

  /**
   * Not implemented in Kafka 0.8. See KAFKA-993
   *
   * {{{
   * OffsetCommitRequest => ConsumerGroup [TopicName [Partition Offset Metadata]]
   * }}}
   */
  private def encodeOffsetCommitRequest(req: OffsetCommitRequest) = {
    val buf = ChannelBuffers.dynamicBuffer() // TODO: estimatedLength
    encodeRequestHeader(buf, ApiKeyOffsetCommit, req)

    buf.encodeString(req.consumerGroup)

    val topics = req.partitions.groupBy(_.topicPartition.topic) // group by topicName
    buf.encodeArray(topics) { case (topicName, partitions) =>
      buf.encodeString(topicName)
      buf.encodeArray(partitions) { partition =>
        buf.encodeInt32(partition.topicPartition.partition)
        buf.encodeInt64(partition.offset)
        buf.encodeString(partition.metadata)
      }
    }

    buf
  }

  /**
   * Not implemented in Kafka 0.8. See KAFKA-993
   *
   * {{{
   * OffsetFetchRequest => ConsumerGroup [TopicName [Partition]]
   * }}}
   */
  private def encodeOffsetFetchRequest(req: OffsetFetchRequest) = {
    val buf = ChannelBuffers.dynamicBuffer() // TODO: estimatedLength
    encodeRequestHeader(buf, ApiKeyOffsetFetch, req)

    buf.encodeString(req.consumerGroup)

    val topics = req.partitions.groupBy(_.topic) // group by topicName
    buf.encodeArray(topics) { case (topicName, partitions) =>
      buf.encodeString(topicName)
      buf.encodeArray(partitions) { partition =>
        buf.encodeInt32(partition.partition)
      }
    }

    buf
  }

}
