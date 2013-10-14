package okapies.finagle.kafka
package protocol

import java.nio.ByteBuffer

import scala.collection.immutable.ListMap

import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}
import org.jboss.netty.handler.codec.embedder.DecoderEmbedder

import okapies.finagle.kafka.util.GatheringByteChannelMock

import org.scalatest._
import org.scalatest.matchers._

class KafkaFrameDecoderTest extends FlatSpec with ShouldMatchers {

  import kafka.api.{
    FetchResponse => KafkaFetchResponse,
    FetchResponsePartitionData => KafkaFetchResponsePartitionData,
    FetchResponseSend => KafkaFetchResponseSend,
    ProducerResponse => KafkaProducerResponse,
    ProducerResponseStatus => KafkaProducerResponseStatus
  }
  import kafka.common.TopicAndPartition
  import kafka.message.{ByteBufferMessageSet, Message => KafkaMessage}
  import Spec._
  import util.Helper._

  behavior of "A KafkaFrameDecoder"

  it should "decode the received ProduceResponse into a ResponseFrame" in {
    val embedder = new DecoderEmbedder[KafkaFrame](
      new KafkaFrameDecoder(_ => Some(ApiKeyProduce), 1024)
    )
    val headerLength = 4 /* Size */ + 4 /* CorrelationId */

    // do multiple times to detect state init failure
    for (i <- 0 to 1) {
      val buf1 = createProduceResponseAsChannelBuffer(i)
      embedder.offer(buf1.duplicate())
      val ResponseFrame(apiKey1, correlationId1, frame1) =
        embedder.poll().asInstanceOf[ResponseFrame]

      assert(apiKey1 === ApiKeyProduce)

      assert(correlationId1 === i)
      assert(frame1 ===
        buf1.slice(buf1.readerIndex + headerLength, buf1.readableBytes - headerLength))
    }
  }

  it should "decode the received FetchResponse into a ResponseFrame and subsequent MessageFrames" in {
    val embedder = new DecoderEmbedder[KafkaFrame](
      new KafkaFrameDecoder(_ => Some(ApiKeyFetch), 1024)
    )

    // do multiple times to detect state init failure
    for (i <- 0 to 1) {
      val buf1 = createFetchResponseAsChannelBuffer(i)
      embedder.offer(buf1.duplicate())
      val ResponseFrame(apiKey1, correlationId1, frame1) =
        embedder.poll().asInstanceOf[ResponseFrame]

      assert(apiKey1 === ApiKeyFetch)

      assert(correlationId1 === i)
      assert(frame1 === ChannelBuffers.EMPTY_BUFFER)

      // 1st partition in the response
      val PartitionFrame(topicPartition1, errorCode1, highwaterMarkOffset1) =
        embedder.poll().asInstanceOf[PartitionFrame]
      assert(topicPartition1 === TopicPartition("test-topic2", 3 + i))
      assert(errorCode1 === 2)
      assert(highwaterMarkOffset1 === 2 + i)

      // 1st message in the response
      val MessageFrame(topicPartition11, offset11, frame11) =
        embedder.poll().asInstanceOf[MessageFrame]
      assert(topicPartition11 === TopicPartition("test-topic2", 3 + i))
      assert(offset11 === 0)
      assert(new String(getMessageValue(frame11), utf8) === "hello")

      // 2nd message in the response
      val MessageFrame(topicPartition12, offset12, frame12) =
        embedder.poll().asInstanceOf[MessageFrame]
      assert(topicPartition12 === TopicPartition("test-topic2", 3 + i))
      assert(offset12 === 1)
      assert(new String(getMessageValue(frame12), utf8) === "world")

      // 2nd partition in the response
      val PartitionFrame(topicPartition2, errorCode2, highwaterMarkOffset2) =
        embedder.poll().asInstanceOf[PartitionFrame]
      assert(topicPartition2 === TopicPartition("test-topic1", 1 + i))
      assert(errorCode2 === 1)
      assert(highwaterMarkOffset2 === 1 + i)

      // 3rd message in the response
      val MessageFrame(topicPartition21, offset21, frame21) =
        embedder.poll().asInstanceOf[MessageFrame]
      assert(topicPartition21 === TopicPartition("test-topic1", 1 + i))
      assert(offset21 === 0)
      assert(new String(getMessageValue(frame21), utf8) === "welcome")

      // 3rd partition in the response
      val PartitionFrame(topicPartition3, errorCode3, highwaterMarkOffset3) =
        embedder.poll().asInstanceOf[PartitionFrame]
      assert(topicPartition3 === TopicPartition("test-topic1", 2 + i))
      assert(errorCode3 === 1)
      assert(highwaterMarkOffset3 === 1 + i)

      // 4th message in the response
      val MessageFrame(topicPartition31, offset31, frame31) =
        embedder.poll().asInstanceOf[MessageFrame]
      assert(topicPartition31 === TopicPartition("test-topic1", 2 + i))
      assert(offset31 === 0)
      assert(new String(getMessageValue(frame31), utf8) === "welcome")

      val msg6 = embedder.poll()
      assert(msg6 === NilMessageFrame)
    }
  }

  private[this] def getMessageValue(frame: ChannelBuffer): Array[Byte] = {
    val payload = new KafkaMessage(frame.toByteBuffer).payload
    val value = new Array[Byte](payload.limit)
    payload.get(value)

    value
  }

  private[this] def createProduceResponseAsChannelBuffer(i: Int) = {
    val status1 = KafkaProducerResponseStatus(1, 1 + i)
    val status2 = KafkaProducerResponseStatus(2, 2 + i)
    val res = KafkaProducerResponse(
      correlationId = i,
      status = ListMap(TopicAndPartition("test-topic1", 1 + i) -> status1) +
        (TopicAndPartition("test-topic1", 2 + i) -> status1) +
        (TopicAndPartition("test-topic2", 3 + i) -> status2)
    )

    val buf = ByteBuffer.allocateDirect(4 /* Size */ + res.sizeInBytes)
    buf.putInt(res.sizeInBytes)
    res.writeTo(buf)
    buf.rewind()

    ChannelBuffers.wrappedBuffer(buf)
  }

  private[this] def createFetchResponseAsChannelBuffer(i: Int) = {
    val data1 = KafkaFetchResponsePartitionData(1, 1 + i, new ByteBufferMessageSet(
      new KafkaMessage("welcome".getBytes(utf8))
    ))
    val data2 = KafkaFetchResponsePartitionData(2, 2 + i, new ByteBufferMessageSet(
      new KafkaMessage("hello".getBytes(utf8)),
      new KafkaMessage("world".getBytes(utf8))
    ))
    val res = KafkaFetchResponse(
      correlationId = i,
      data = ListMap(TopicAndPartition("test-topic1", 1 + i) -> data1) +
        (TopicAndPartition("test-topic1", 2 + i) -> data1) +
        (TopicAndPartition("test-topic2", 3 + i) -> data2)
    )

    val buf = ByteBuffer.allocateDirect(4 /* Size */ + res.sizeInBytes)
    new KafkaFetchResponseSend(res).writeTo(new GatheringByteChannelMock(buf))
    buf.rewind()

    ChannelBuffers.wrappedBuffer(buf)
  }

}
