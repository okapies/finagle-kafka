package okapies.finagle.kafka.protocol

import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.{Channel, ChannelHandlerContext, ChannelOutboundHandlerAdapter, ChannelPromise, ChannelFutureListener, ChannelFuture}
import io.netty.handler.codec.{MessageToMessageDecoder, MessageToMessageEncoder}

import java.util.{List => JList}

class RequestDecoder extends MessageToMessageDecoder[ByteBuf] {

  import Spec._

  def decode(ctx: ChannelHandlerContext, frame: ByteBuf, out: JList[AnyRef]): Unit = {
    val (apiKey, version, correlationId, clientId) = decodeRequestHeader(frame)

    val req = apiKey match {
      case ApiKeyProduce => decodeProduceRequest(correlationId, clientId, frame)
      case ApiKeyFetch => decodeFetchRequest(correlationId, clientId, frame)
      case ApiKeyOffset => decodeOffsetRequest(correlationId, clientId, frame)
      case ApiKeyMetadata => decodeMetadataRequest(correlationId, clientId, frame)
      case ApiKeyLeaderAndIsr => null // TODO: not implemented yet
      case ApiKeyStopReplica => null  // TODO: not implemented yet
      case ApiKeyOffsetCommit =>
        version match {
          case 0 => decodeOffsetCommitRequestV0(correlationId, clientId, frame)
          case _ => null // not yet supporting v1, v2
        }

      case ApiKeyOffsetFetch => decodeOffsetFetchRequest(correlationId, clientId, frame)
      case ApiKeyConsumerMetadata => decodeConsumerMetadataRequest(correlationId, clientId, frame)
    }

    out.add(req)
  }

  private def decodeRequestHeader(buf: ByteBuf): (Short, Short, Int, String) = {
    val apiKey = buf.decodeInt16()
    val version = buf.decodeInt16()
    val correlationId = buf.decodeInt32()
    val clientId = buf.decodeString()
    (apiKey, version, correlationId, clientId)
  }

  /**
   * {{{
   * ProduceRequest => RequiredAcks Timeout [TopicName [Partition MessageSetSize MessageSet]]
   *   MessageSet => [Offset MessageSize Message]
   * }}}
   */
  private def decodeProduceRequest(corrId: Int, clientId: String, frame: ByteBuf): ProduceRequest = {
    val acks = RequiredAcks(frame.decodeInt16())
    val timeout = frame.decodeInt32()

    val topics = frame.decodeArray {
      val name = frame.decodeString()

      val partitions = frame.decodeArray {
        val partition = frame.decodeInt32()

        // decodes MessageSize and MessageSet
        val msgs = frame.decodeMessageSet()
        (partition -> msgs.map(_.message))
      }.toMap

      (name -> partitions)
    }.toMap

    ProduceRequest(corrId, clientId, acks, timeout, topics)
  }

  /**
   * {{{
   * FetchRequest => ReplicaId MaxWaitTime MinBytes [TopicName [Partition FetchOffset MaxBytes]]
   * }}}
   */
  private def decodeFetchRequest(corrId: Int, clientId: String, frame: ByteBuf): FetchRequest = {
    val replicaId = frame.decodeInt32()
    val maxWaitTime = frame.decodeInt32()
    val minBytes = frame.decodeInt32()

    val topicPartitions = frame.decodeArray {
      val topic = frame.decodeString()

      val partitions = frame.decodeArray {
        val partition = frame.decodeInt32()
        val offset = frame.decodeInt64()
        val maxBytes = frame.decodeInt32()

        (partition -> FetchOffset(offset, maxBytes))
      }.toMap

      (topic -> partitions)
    }.toMap

    FetchRequest(corrId, clientId, replicaId, maxWaitTime, minBytes,topicPartitions)
  }

  /**
   * {{{
   * OffsetRequest => ReplicaId [TopicName [Partition Time MaxNumberOfOffsets]]
   * }}}
   */
  private def decodeOffsetRequest(corrId: Int, clientId: String, frame: ByteBuf): OffsetRequest = {
    val replicaId = frame.decodeInt32()

    val topicPartitions = frame.decodeArray {
      val name = frame.decodeString()

      val partitions = frame.decodeArray {
        val partition = frame.decodeInt32()
        val time = frame.decodeInt64()
        val maxOffsets = frame.decodeInt32()

        (partition -> OffsetFilter(time, maxOffsets))
      }.toMap

      (name -> partitions)
    }.toMap

    OffsetRequest(corrId, clientId, replicaId, topicPartitions)
  }

  /**
   * {{{
   * MetadataRequest => [TopicName]
   * }}}
   */
  private def decodeMetadataRequest(corrId: Int, clientId: String, frame: ByteBuf): MetadataRequest = {
    val topics = frame.decodeArray {
      frame.decodeString()
    }

    MetadataRequest(corrId, clientId, topics)
  }

  /**
   * {{{
   * OffsetCommitRequest => ConsumerGroupId [TopicName [Partition Offset Metadata]]
   * }}}
   *
   * v0, kafka 0.8.1 and later
   */
  private def decodeOffsetCommitRequestV0(
    corrId: Int,
    clientId: String,
    frame: ByteBuf): OffsetCommitRequest = {
    
    val consumerGroup = frame.decodeString()

    val topicPartitions = frame.decodeArray {
      val name = frame.decodeString()

      val partition = frame.decodeArray {
        val partition = frame.decodeInt32()
        val offset = frame.decodeInt64()
        val metadata = frame.decodeString()

        (partition -> CommitOffset(offset, metadata))
      }.toMap

      (name -> partition)
    }.toMap

    OffsetCommitRequest(corrId, clientId, consumerGroup, topicPartitions)
  }

  /**
   * {{{
   * OffsetFetchRequest => ConsumerGroup [TopicName [Partition]]
   * }}}
   */
  private def decodeOffsetFetchRequest(corrId: Int, clientId: String, frame: ByteBuf): OffsetFetchRequest = {
    val consumerGroup = frame.decodeString()

    val topicPartitions = frame.decodeArray {
      val topic = frame.decodeString()

      val partitions = frame.decodeArray {
        frame.decodeInt32()
      }

      (topic -> partitions)
    }.toMap

    OffsetFetchRequest(corrId, clientId, consumerGroup, topicPartitions)
  }

  /**
   * {{{
   * ConsumerMetadataRequest => ConsumerGroup
   * }}}
   */
  private def decodeConsumerMetadataRequest(
    corrId: Int,
    clientId: String,
    frame: ByteBuf): ConsumerMetadataRequest = {

    val consumerGroup = frame.decodeString()

    ConsumerMetadataRequest(corrId, clientId, consumerGroup)
  }


}

class RequestEncoder(logger: RequestLogger) extends ChannelOutboundHandlerAdapter {

  import Spec._

  override def write(ctx: ChannelHandlerContext, msg: Any, promise: ChannelPromise) {
    msg match {
      case req: Request =>
        val buf = msg match {
          case req: ProduceRequest => encodeProduceRequest(req)
          case req: FetchRequest => encodeFetchRequest(req)
          case req: OffsetRequest => encodeOffsetRequest(req)
          case req: MetadataRequest => encodeMetadataRequest(req)
          case req: OffsetCommitRequest => encodeOffsetCommitRequest(req)
          case req: OffsetFetchRequest => encodeOffsetFetchRequest(req)
          case req: ConsumerMetadataRequest => encodeConsumerMetadataRequest(req)
        }

       logger.add(req)

        promise.addListener(new ChannelFutureListener {
          def operationComplete(f: ChannelFuture) {
            if (!f.isSuccess) { // cancelled or failed
              logger.remove(req)
            }
          }
        })

        ctx.writeAndFlush(buf, promise)
      case _ => 
        //super.writeRequested(ctx, e) // fall through
        val ex = new KafkaCodecException("Failed to recognise outgoing request")
        promise.setFailure(ex)
        throw ex

    }
  }

  private def encodeRequestHeader(buf: ByteBuf, apiKey: Short, req: Request) {
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
  private def encodeProduceRequest(req: ProduceRequest): ByteBuf = {
    val buf = Unpooled.buffer() // TODO: estimatedLength
    encodeRequestHeader(buf, ApiKeyProduce, req)

    // TODO: return empty response immediately if requiredAcks == 0
    buf.encodeInt16(req.requiredAcks.count)
    buf.encodeInt32(req.timeout)

    // [TopicName [Partition MessageSetSize MessageSet]]
    buf.encodeArray(req.messageSets) { case (topicName, messageSets) =>
      buf.encodeString(topicName)
      buf.encodeArray(messageSets) { case (partition, messageSet) =>
        buf.encodeInt32(partition)
        buf.encodeMessageSet(messageSet) { message =>
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
  private def encodeFetchRequest(req: FetchRequest): ByteBuf = {
    val buf = Unpooled.buffer() // TODO: estimatedLength
    encodeRequestHeader(buf, ApiKeyFetch, req)

    buf.encodeInt32(req.replicaId)
    buf.encodeInt32(req.maxWaitTime)
    buf.encodeInt32(req.minBytes)

    // [TopicName [Partition FetchOffset MaxBytes]
    buf.encodeArray(req.topicPartitions) { case (topicName, partitions) =>
      buf.encodeString(topicName)
      buf.encodeArray(partitions) { case (partition, offset) =>
        buf.encodeInt32(partition)
        buf.encodeInt64(offset.offset)
        buf.encodeInt32(offset.maxBytes)
      }
    }

    buf
  }

  /**
   * {{{
   * OffsetRequest => ReplicaId [TopicName [Partition Time MaxNumberOfOffsets]]
   * }}}
   */
  private def encodeOffsetRequest(req: OffsetRequest): ByteBuf = {
    val buf = Unpooled.buffer() // TODO: estimatedLength
    encodeRequestHeader(buf, ApiKeyOffset, req)

    buf.encodeInt32(req.replicaId)
    buf.encodeArray(req.topicPartitions) { case (topicName, partitions) =>
      buf.encodeString(topicName)
      buf.encodeArray(partitions) { case (partition, filter) =>
        buf.encodeInt32(partition)
        buf.encodeInt64(filter.time)
        buf.encodeInt32(filter.maxNumberOfOffsets)
      }
    }

    buf
  }

  /**
   * {{{
   * MetadataRequest => [TopicName]
   * }}}
   */
  private def encodeMetadataRequest(req: MetadataRequest): ByteBuf = {
    val buf = Unpooled.buffer() // TODO: estimatedLength
    encodeRequestHeader(buf, ApiKeyMetadata, req)

    buf.encodeStringArray(req.topics) // [TopicName]

    buf
  }

  /**
   * Not implemented in Kafka 0.8. See KAFKA-993
   * Implemented in Kafka 0.8.1
   *
   * {{{
   * OffsetCommitRequest => ConsumerGroup [TopicName [Partition Offset Metadata]]
   * }}}
   */
  private def encodeOffsetCommitRequest(req: OffsetCommitRequest): ByteBuf = {
    val buf = Unpooled.buffer() // TODO: estimatedLength
    encodeRequestHeader(buf, ApiKeyOffsetCommit, req)

    buf.encodeString(req.consumerGroup)

    buf.encodeArray(req.commits) { case (topicName, partitions) =>
      buf.encodeString(topicName)
      buf.encodeArray(partitions) { case (partition, commit) =>
        buf.encodeInt32(partition)
        buf.encodeInt64(commit.offset)
        buf.encodeString(commit.metadata)
      }
    }

    buf
  }

  /**
   * Not implemented in Kafka 0.8. See KAFKA-993
   * Implemented in Kafka 0.8.1
   *
   * {{{
   * OffsetFetchRequest => ConsumerGroup [TopicName [Partition]]
   * }}}
   */
  private def encodeOffsetFetchRequest(req: OffsetFetchRequest): ByteBuf = {
    val buf = Unpooled.buffer() // TODO: estimatedLength
    encodeRequestHeader(buf, ApiKeyOffsetFetch, req)

    buf.encodeString(req.consumerGroup)

    buf.encodeArray(req.partitions) { case (topicName, partitions) =>
      buf.encodeString(topicName)
      buf.encodeArray(partitions) { partition =>
        buf.encodeInt32(partition)
      }
    }

    buf
  }

  /**
   * Implemented in Kafka 0.8.2.0
   *
   * {{{
   * ConsumerMetadataRequest => ConsumerGroup
   * }}}
   */
  private def encodeConsumerMetadataRequest(req: ConsumerMetadataRequest): ByteBuf = {
    val buf = Unpooled.buffer() // TODO: estimatedLength
    encodeRequestHeader(buf, ApiKeyConsumerMetadata, req)

    buf.encodeString(req.consumerGroup)

    buf
  }


}
