package okapies.finagle.kafka.protocol

import scala.annotation.tailrec

import io.netty.buffer.{ByteBuf, ByteBufUtil}
import io.netty.channel.{Channel, ChannelHandlerContext}
import io.netty.handler.codec.ReplayingDecoder

import java.util.{List => JList}

import Spec._
import KafkaFrameDecoderState._

class StreamFrameDecoder(
  correlator: RequestCorrelator,
  maxFrameLength: Int
) extends ReplayingDecoder[KafkaFrameDecoderState](READ_HEADER) {

  private[this] var size: Int32 = _

  /* context of fetched messages */

  private[this] var readSize: Int32 = _

  private[this] var topicCount: Int32 = _

  private[this] var readTopicCount: Int32 = _

  private[this] var topicName: String = _

  private[this] var partitionCount: Int32 = _

  private[this] var readPartitionCount: Int32 = _

  private[this] var partition: Int32 = _

  private[this] var errorCode: Int16 = _

  private[this] var highwaterMarkOffset: Int64 = _

  private[this] var messageSetSize: Int32 = _

  private[this] var readMessageSetSize: Int32 = _

  @tailrec
  override final def decode(
      ctx: ChannelHandlerContext,
      buffer: ByteBuf,
      out: JList[AnyRef]): Unit = state() match {
    case READ_HEADER =>
      size = buffer.decodeInt32()          // int32
      val correlationId = buffer.decodeInt32() // int32

      correlator(correlationId) match {
        case Some(ApiKeyFetch) =>
          readSize = CorrelationIdLength
          checkpoint(READ_TOPIC_COUNT)

          out.add(FetchResponseFrame(correlationId))
        case Some(apiKey) =>
          // read bytes without Size and CorrelationId
          val frame = buffer.readBytes(size - CorrelationIdLength)
          checkpoint(READ_HEADER) // back to init state

          out.add(BufferResponseFrame(apiKey, correlationId, frame))
        case _ =>
          throw new KafkaCodecException("Unrecognized type of response correlated to the request.")
      }

    // [TopicName [Partition ErrorCode HighwaterMarkOffset MessageSetSize MessageSet]]
    //   MessageSet => [Offset MessageSize Message]
    case READ_TOPIC_COUNT =>
      topicCount = buffer.decodeInt32() // int32

      readSize += 4
      readTopicCount = 0
      checkpoint(READ_TOPIC)

      decode(ctx, buffer, out)
    case READ_TOPIC =>
      topicCount - readTopicCount match { // Topic
        case n if n > 0 =>
          val prev = buffer.readerIndex
          topicName = buffer.decodeString() // string (int16 + bytes)

          val length = buffer.readerIndex - prev
          readSize += length
          readTopicCount += 1
          checkpoint(READ_PARTITION_COUNT)

          decode(ctx, buffer, out)
        case n if n == 0 =>
          state(READ_HEADER) // back to init state

          out.add(NilMessageFrame) // indicates end of stream
        case _ =>
          throw new KafkaCodecException(
            "The response has illegal number of topics: expected=%d, actual=%d"
              .format(topicCount, readTopicCount))
      }
    case READ_PARTITION_COUNT =>
      partitionCount = buffer.decodeInt32() // int32

      readSize += 4
      readPartitionCount = 0
      checkpoint(READ_PARTITION)

      decode(ctx, buffer, out)
    case READ_PARTITION =>
      partitionCount - readPartitionCount match { // Partition
        case n if n > 0 =>
          partition = buffer.decodeInt32()       // int32
          errorCode = buffer.decodeInt16()           // int16
          highwaterMarkOffset = buffer.decodeInt64() // int64

          readSize +=
            4 + /* Partition */
            2 + /* ErrorCode */
            8   /* HighwaterMarkOffset */
          readPartitionCount += 1
          checkpoint(READ_MESSAGE_SET)

          out.add(PartitionStatus(topicName, partition, errorCode, highwaterMarkOffset))
        case n if n == 0 =>
          state(READ_TOPIC)
          decode(ctx, buffer, out)
        case _ =>
          throw new KafkaCodecException(
            "The response has illegal number of partitions: expected=%d, actual=%d"
              .format(partitionCount, readPartitionCount))
      }
    case READ_MESSAGE_SET =>
      messageSetSize = buffer.decodeInt32() // int32

      readSize += 4
      readMessageSetSize = 0
      checkpoint(READ_MESSAGE)

      decode(ctx, buffer, out)
    case READ_MESSAGE =>
      messageSetSize - readMessageSetSize match { // MessageSet
        case n if n > 0 =>
          val prev = buffer.readerIndex
          val offset = buffer.decodeInt64() // int64
          val frame = buffer.decodeBytes()  // int32 + bytes

          val length = buffer.readerIndex - prev
          readSize += length
          readMessageSetSize += length

          val messageFrame = FetchedMessage(topicName, partition, offset, Message(frame))
          size - readSize match {
            case n if n > 0 =>
              checkpoint() // continue
          
              out.add(messageFrame)
            case n if n == 0 =>
              checkpoint(READ_HEADER) // back to init state

              // output the frame and end of stream
              out.add(messageFrame)
              out.add(NilMessageFrame) // indicates end of stream
            case n if n < 0 =>
              throw new KafkaCodecException(
                "The response has illegal size: expected=%d, actual=%d"
                  .format(size, readSize))
          }
        case n if n == 0 =>
          state(READ_PARTITION)
          decode(ctx, buffer, out)
        case _ =>
          throw new KafkaCodecException(
            "The response has illegal MessageSet size: expected=%d, actual=%d"
              .format(messageSetSize, readMessageSetSize))
      }

    case _ => throw new KafkaCodecException("The decoder fall into undefined state.")
  }

}
