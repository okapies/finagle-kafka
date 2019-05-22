package okapies.finagle.kafka.protocol

import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.{Channel, ChannelHandlerContext}
import io.netty.handler.codec.{MessageToMessageEncoder, MessageToMessageDecoder}

import java.util.{List => JList}

class BatchResponseDecoder(correlator: RequestCorrelator) extends MessageToMessageDecoder[ByteBuf] {

  import ResponseDecoder._
  import Spec._


  override def decode(ctx: ChannelHandlerContext, msg: ByteBuf, out: JList[AnyRef]): Unit = {
      // Note: MessageSize field must be discarded by previous frame/msg decoder.
      val correlationId = msg.decodeInt32()
      for(apiKey <- correlator(correlationId)) {
        out.add(decodeResponse(apiKey, correlationId, msg))
      }
  }

}

class StreamResponseDecoder extends MessageToMessageDecoder[ResponseFrame] {

  import com.twitter.concurrent.{Broker => TwitterBroker}
  import com.twitter.util.Promise

  import ResponseDecoder._
  import Spec._

  private[this] var partitions: TwitterBroker[PartitionStatus] = null

  private[this] var messages: TwitterBroker[FetchedMessage] = null

  private[this] var complete: Promise[Unit] = null

  override def decode(ctx: ChannelHandlerContext, msg: ResponseFrame, out: JList[AnyRef]): Unit = msg match {
    case BufferResponseFrame(apiKey, correlationId, frame) =>
      out.add(decodeResponse(apiKey, correlationId, frame))

    case FetchResponseFrame(correlationId) =>
      partitions = new TwitterBroker[PartitionStatus]
      messages = new TwitterBroker[FetchedMessage]
      complete = new Promise[Unit]

      out.add(StreamFetchResponse(correlationId, partitions.recv, messages.recv, complete))
    case partition: PartitionStatus =>
      partitions ! partition

      //null
    case msg: FetchedMessage =>
      messages ! msg

      //null
    case NilMessageFrame =>
      complete.setValue(())

      partitions = null
      messages = null
      complete = null

      //null
    case _ => // fall through
  }

}

object ResponseDecoder {

  import Spec._

  def decodeResponse(apiKey: Short, correlationId: Int, frame: ByteBuf) = apiKey match {
    case ApiKeyProduce => decodeProduceResponse(correlationId, frame)
    case ApiKeyFetch => decodeFetchResponse(correlationId, frame)
    case ApiKeyOffset => decodeOffsetResponse(correlationId, frame)
    case ApiKeyMetadata => decodeMetadataResponse(correlationId, frame)
    case ApiKeyLeaderAndIsr => null
    case ApiKeyStopReplica => null
    case ApiKeyOffsetCommit => decodeOffsetCommitResponse(correlationId, frame)
    case ApiKeyOffsetFetch => decodeOffsetFetchResponse(correlationId, frame)
    case ApiKeyConsumerMetadata => decodeConsumerMetadataResponse(correlationId, frame)
  }

  /**
   * {{{
   * ProduceResponse => [TopicName [Partition ErrorCode Offset]]
   * }}}
   */
  private[this] def decodeProduceResponse(correlationId: Int, buf: ByteBuf) = {
    val results = buf.decodeArray {
      val topicName = buf.decodeString()

      // [Partition ErrorCode Offset]
      val partitions = buf.decodeArray {
        val partition = buf.decodeInt32()
        val error: KafkaError = buf.decodeInt16()
        val offset = buf.decodeInt64()

        partition -> ProduceResult(error, offset)
      }.toMap

      topicName -> partitions
    }.toMap

    ProduceResponse(correlationId, results)
  }

  /**
   * {{{
   * FetchResponse => [TopicName [Partition ErrorCode HighwaterMarkOffset MessageSetSize MessageSet]]
   * }}}
   */
  private[this] def decodeFetchResponse(correlationId: Int, buf: ByteBuf) = {
    val results = buf.decodeArray {
      val topicName = buf.decodeString()

      // [Partition ErrorCode HighwaterMarkOffset MessageSetSize MessageSet]
      val partitions = buf.decodeArray {
        val partition = buf.decodeInt32()
        val error: KafkaError = buf.decodeInt16()
        val highwaterMarkOffset = buf.decodeInt64()
        val messages = buf.decodeMessageSet()

        partition -> FetchResult(error, highwaterMarkOffset, messages)
      }.toMap

      topicName -> partitions
    }.toMap

    FetchResponse(correlationId, results)
  }

  /**
   * {{{
   * OffsetResponse => [TopicName [PartitionOffsets]]
   *   PartitionOffsets => Partition ErrorCode [Offset]
   * }}}
   */
  private[this] def decodeOffsetResponse(correlationId: Int, buf: ByteBuf) = {
    val results = buf.decodeArray {
      val topicName = buf.decodeString()

      // [Partition ErrorCode [Offset]]
      val partitions = buf.decodeArray {
        val partition = buf.decodeInt32()
        val error: KafkaError = buf.decodeInt16()
        val offsets = buf.decodeArray(buf.decodeInt64())

        partition -> OffsetResult(error, offsets)
      }.toMap

      topicName -> partitions
    }.toMap

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
  private[this] def decodeMetadataResponse(correlationId: Int, buf: ByteBuf) = {
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
   * Implemented in Kafka 0.8.1
   *
   * {{{
   * OffsetCommitResponse => [TopicName [Partition ErrorCode]]]
   * }}}
   */
  private[this] def decodeOffsetCommitResponse(correlationId: Int, buf: ByteBuf) = {
    val results = buf.decodeArray {
      val topicName = buf.decodeString()

      // [Partition ErrorCode [Offset]]
      val partitions = buf.decodeArray {
        val partition = buf.decodeInt32()
        val error: KafkaError = buf.decodeInt16()

        partition -> OffsetCommitResult(error)
      }.toMap

      topicName -> partitions
    }.toMap

    OffsetCommitResponse(correlationId, results)
  }

  /**
   * Not implemented in Kafka 0.8. See KAFKA-993
   * Implemented in Kafka 0.8.1
   *
   * {{{
   * OffsetFetchResponse => [TopicName [Partition Offset Metadata ErrorCode]]
   * }}}
   */
  private[this] def decodeOffsetFetchResponse(correlationId: Int, buf: ByteBuf) = {
    val results = buf.decodeArray {
      val topicName = buf.decodeString()

      // [Partition ErrorCode [Offset]]
      val partitions = buf.decodeArray {
        val partition = buf.decodeInt32()
        val offset = buf.decodeInt64()
        val metadata = buf.decodeString()
        val error: KafkaError = buf.decodeInt16()

        partition -> OffsetFetchResult(offset, metadata, error)
      }.toMap

      topicName -> partitions
    }.toMap

    OffsetFetchResponse(correlationId, results)
  }

  /**
   * Implemented in Kafka 0.8.2.0
   *
   * {{{
   * ConsumerMetadataResponse => ErrorCode CoordinatorId CoordinatorHost CoordinatorPort
   * }}}
   */
  private[this] def decodeConsumerMetadataResponse(correlationId: Int, buf: ByteBuf) = {
    val error: KafkaError = buf.decodeInt16()
    val id = buf.decodeInt32()
    val host = buf.decodeString()
    val port = buf.decodeInt32()

    val result = ConsumerMetadataResult(error, id, host, port)
    ConsumerMetadataResponse(correlationId, result)
  }
}

class ResponseEncoder extends MessageToMessageEncoder[Response] {

  import Spec._

  override def encode(ctx: ChannelHandlerContext, msg: Response, out: JList[AnyRef]): Unit = {
    val buf = msg match {
      case resp: MetadataResponse => encodeMetadataResponse(resp)
      case resp: ProduceResponse => encodeProduceResponse(resp)
      case resp: OffsetResponse => encodeOffsetResponse(resp)
      case resp: FetchResponse => encodeFetchResponse(resp)
      case resp: OffsetCommitResponse => encodeOffsetCommitResponse(resp)
      case resp: OffsetFetchResponse => encodeOffsetFetchResponse(resp)
      case resp: ConsumerMetadataResponse => encodeConsumerMetadataResponse(resp)
    }

    out.add(buf)
  }

  private def encodeResponseHeader(buf: ByteBuf, resp: Response) {
    buf.encodeInt32(resp.correlationId) // CorrelationId => int32
  }

  private def encodeBroker(buf: ByteBuf, broker: Broker) {
    buf.encodeInt32(broker.nodeId)
    buf.encodeString(broker.host)
    buf.encodeInt32(broker.port)
  }

  /**
   * {{{
   * MetadataResponse => [Broker][TopicMetadata]
   *   Broker => NodeId Host Port
   *   TopicMetadata => TopicErrorCode TopicName [PartitionMetadata]
   *     PartitionMetadata => PartitionErrorCode PartitionId Leader [Replicas] [Isr]
   * }}}
   */
  private def encodeMetadataResponse(resp: MetadataResponse) = {
    val buf = Unpooled.buffer()

    encodeResponseHeader(buf, resp)

    //  [Broker]
    buf.encodeArray(resp.brokers) { broker =>
      encodeBroker(buf, broker)
    }

    // [TopicMetadata]

    buf.encodeArray(resp.topics) { topic =>
      buf.encodeInt16(topic.error.code)
      buf.encodeString(topic.name)

      // [PartitionMetadata]
      buf.encodeArray(topic.partitions) { partition =>
        buf.encodeInt16(partition.error.code)
        buf.encodeInt32(partition.id)

        // id of leader broker or -1 if during leader election
        val leaderId = partition.leader.map(_.nodeId).getOrElse(-1)
        buf.encodeInt32(leaderId)

        buf.encodeArray(partition.replicas) { broker =>
          buf.encodeInt32(broker.nodeId)
        }

        buf.encodeArray(partition.isr) { broker =>
          buf.encodeInt32(broker.nodeId)
        }
      }
    }

    buf
  }

  /**
   * {{{
   * ProduceResponse => [TopicName [Partition ErrorCode Offset]]
   * }}}
   */
  private def encodeProduceResponse(resp: ProduceResponse) = {
    val buf = Unpooled.buffer()

    encodeResponseHeader(buf, resp)

    buf.encodeArray(resp.results) { case (topic, partitions) =>
      buf.encodeString(topic)

      buf.encodeArray(partitions) { case (id, partition) =>
        buf.encodeInt32(id)
        buf.encodeInt16(partition.error.code)
        buf.encodeInt64(partition.offset)
      }
    }

    buf
  }

  /**
   * {{{
   * OffsetResponse => [TopicName [PartitionOffsets]]
   * }}}
   */
  private def encodeOffsetResponse(resp: OffsetResponse) = {
    val buf = Unpooled.buffer()

    encodeResponseHeader(buf, resp)

    buf.encodeArray(resp.results) { case (topic, partitions) =>
      buf.encodeString(topic)

      buf.encodeArray(partitions) { case (partition, offsetResults) =>
        buf.encodeInt32(partition)
        buf.encodeInt16(offsetResults.error.code)

        buf.encodeArray(offsetResults.offsets) { case offset =>
          buf.encodeInt64(offset)
        }
      }
    }

    buf
  }

  /**
   * {{{
   * FetchResponse => [TopicName [Partition ErrorCode HighwaterMarkOffset MessageSetSize MessageSet]]
   * }}}
   */
  private def encodeFetchResponse(resp: FetchResponse) = {
    val buf = Unpooled.buffer()

    encodeResponseHeader(buf, resp)

    buf.encodeArray(resp.results) { case (topic, partitions) =>
      buf.encodeString(topic)

      buf.encodeArray(partitions) { case (partition, fetchResult) =>
        buf.encodeInt32(partition)

        buf.encodeInt16(fetchResult.error.code)
        buf.encodeInt64(fetchResult.highwaterMarkOffset)

        buf.encodeMessageSetWithOffset(fetchResult.messages) { msg =>
          buf.encodeInt64(msg.offset)
          buf.encodeBytes(msg.message.underlying)
        }
      }
    }

    buf
  }

  /**
   * {{{
   * OffsetCommitResponse => [TopicName [Partition ErrorCode]]]
   * }}}
   */
  private def encodeOffsetCommitResponse(resp: OffsetCommitResponse) = {
    val buf = Unpooled.buffer()

    encodeResponseHeader(buf, resp)

    buf.encodeArray(resp.results) { case (topic, partitions) =>
      buf.encodeString(topic)

      buf.encodeArray(partitions) { case (partition, commitResult) =>
        buf.encodeInt32(partition)
        buf.encodeInt16(commitResult.error.code)
      }
    }

    buf
  }

  /**
   * {{{
   * OffsetFetchResponse => [TopicName [Partition Offset Metadata ErrorCode]]
   * }}}
   */
  private def encodeOffsetFetchResponse(resp: OffsetFetchResponse) = {
    val buf = Unpooled.buffer()

    encodeResponseHeader(buf, resp)

    buf.encodeArray(resp.results) { case (topic, partitions) =>
      buf.encodeString(topic)

      buf.encodeArray(partitions) { case (partition, fetchResult) =>
        buf.encodeInt32(partition)
        buf.encodeInt64(fetchResult.offset)
        buf.encodeString(fetchResult.metadata)
        buf.encodeInt16(fetchResult.error.code)
      }
    }

    buf
  }

  /**
   * {{{
   * ConsumerMetadataResponse => ErrorCode CoordinatorId CoordinatorHost CoordinatorPort
   * }}}
   */
  private def encodeConsumerMetadataResponse(resp: ConsumerMetadataResponse) = {
    val buf = Unpooled.buffer()

    encodeResponseHeader(buf, resp)

    buf.encodeInt16(resp.result.error.code)
    buf.encodeInt32(resp.result.id)
    buf.encodeString(resp.result.host)
    buf.encodeInt32(resp.result.port)

    buf
  }
}
