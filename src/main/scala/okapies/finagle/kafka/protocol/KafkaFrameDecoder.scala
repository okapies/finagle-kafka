package okapies.finagle.kafka.protocol

import scala.annotation.tailrec

import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.channel.{Channel, ChannelHandlerContext}
import org.jboss.netty.handler.codec.replay.ReplayingDecoder

import Spec._
import KafkaFrameDecoderState._

sealed trait KafkaFrame

case class ResponseFrame(
  apiKey: Int16,
  correlationId: Int32,
  frame: ChannelBuffer) extends KafkaFrame

case class PartitionFrame(
  topicPartition: TopicPartition,
  errorCode: Int16,
  highwaterMarkOffset: Int64
) extends KafkaFrame

case class MessageFrame(
  topicPartition: TopicPartition,
  offset: Int64,
  frame: ChannelBuffer) extends KafkaFrame

case object NilMessageFrame extends KafkaFrame

class KafkaFrameDecoder(
  apiKeyByReq: (Int32 => Option[Int16]),
  maxFrameLength: Int
) extends ReplayingDecoder[KafkaFrameDecoderState](READ_HEADER, true /* unfold */) {

  private[this] var size: Int32 = _

  /* context of fetched messages */

  private[this] var readSize: Int32 = _

  private[this] var topicCount: Int32 = _

  private[this] var readTopicCount: Int32 = _

  private[this] var topicName: String = _

  private[this] var partitionCount: Int32 = _

  private[this] var readPartitionCount: Int32 = _

  private[this] var topicPartition: TopicPartition = _

  private[this] var errorCode: Int16 = _

  private[this] var highwaterMarkOffset: Int64 = _

  private[this] var messageSetSize: Int32 = _

  private[this] var readMessageSetSize: Int32 = _

  @tailrec
  override final def decode(
      ctx: ChannelHandlerContext,
      channel: Channel,
      buffer: ChannelBuffer,
      state: KafkaFrameDecoderState): AnyRef = state match {
    case READ_HEADER =>
      size = buffer.decodeInt32()          // int32
      val correlationId = buffer.decodeInt32() // int32

      apiKeyByReq(correlationId) match {
        case Some(ApiKeyFetch) =>
          readSize = CorrelationIdLength
          checkpoint(READ_TOPIC_COUNT)

          ResponseFrame(ApiKeyFetch, correlationId, ChannelBuffers.EMPTY_BUFFER)
        case Some(apiKey) =>
          // read bytes without Size and CorrelationId
          val frame = buffer.readBytes(size - CorrelationIdLength)
          checkpoint(READ_HEADER) // back to init state

          ResponseFrame(apiKey, correlationId, frame)
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

      decode(ctx, channel, buffer, READ_TOPIC)
    case READ_TOPIC =>
      topicCount - readTopicCount match { // Topic
        case n if n > 0 =>
          val prev = buffer.readerIndex
          topicName = buffer.decodeString() // string (int16 + bytes)

          val length = buffer.readerIndex - prev
          readSize += length
          readTopicCount += 1
          checkpoint(READ_PARTITION_COUNT)

          decode(ctx, channel, buffer, READ_PARTITION_COUNT)
        case n if n == 0 =>
          setState(READ_HEADER) // back to init state

          NilMessageFrame // indicates end of stream
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

      decode(ctx, channel, buffer, READ_PARTITION)
    case READ_PARTITION =>
      partitionCount - readPartitionCount match { // Partition
        case n if n > 0 =>
          val partition = buffer.decodeInt32()       // int32
          errorCode = buffer.decodeInt16()           // int16
          highwaterMarkOffset = buffer.decodeInt64() // int64

          topicPartition = TopicPartition(topicName, partition)
          readSize +=
            4 + /* Partition */
            2 + /* ErrorCode */
            8   /* HighwaterMarkOffset */
          readPartitionCount += 1
          checkpoint(READ_MESSAGE_SET)

          PartitionFrame(topicPartition, errorCode, highwaterMarkOffset)
        case n if n == 0 =>
          setState(READ_TOPIC)
          decode(ctx, channel, buffer, READ_TOPIC)
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

      decode(ctx, channel, buffer, READ_MESSAGE)
    case READ_MESSAGE =>
      messageSetSize - readMessageSetSize match { // MessageSet
        case n if n > 0 =>
          val prev = buffer.readerIndex
          val offset = buffer.decodeInt64() // int64
          val frame = buffer.decodeBytes()  // int32 + bytes

          val length = buffer.readerIndex - prev
          readSize += length
          readMessageSetSize += length

          val messageFrame = MessageFrame(topicPartition, offset, frame)
          size - readSize match {
            case n if n > 0 =>
              checkpoint() // continue

              messageFrame
            case n if n == 0 =>
              checkpoint(READ_HEADER) // back to init state

              // unfold by ReplayingDecoder
              Array[KafkaFrame](
                messageFrame,
                NilMessageFrame // indicates end of stream
              )
            case n if n < 0 =>
              throw new KafkaCodecException(
                "The response has illegal size: expected=%d, actual=%d"
                  .format(size, readSize))
          }
        case n if n == 0 =>
          setState(READ_PARTITION)
          decode(ctx, channel, buffer, READ_PARTITION)
        case _ =>
          throw new KafkaCodecException(
            "The response has illegal MessageSet size: expected=%d, actual=%d"
              .format(messageSetSize, readMessageSetSize))
      }

    case _ => throw new KafkaCodecException("The decoder fall into undefined state.")
  }

}
