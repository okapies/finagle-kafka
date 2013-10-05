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

      // 1st message in the response
      val MessageFrame(
          correlationId2,
          topicName2, partition2,
          errorCode2, highwaterMarkOffset2, offset2, frame2) =
        embedder.poll().asInstanceOf[MessageFrame]
      assert(correlationId2 === i)
      assert(topicName2 === "test-topic2")
      assert(partition2 === 3 + i)
      assert(errorCode2 === 2)
      assert(highwaterMarkOffset2 === 2 + i)
      assert(offset2 === 0)
      assert(new String(getMessageValue(frame2), "UTF-8") === "hello")

      // 2nd message in the response
      val MessageFrame(
          correlationId3,
          topicName3, partition3,
          errorCode3, highwaterMarkOffset3, offset3, frame3) =
        embedder.poll().asInstanceOf[MessageFrame]
      assert(correlationId3 === i)
      assert(topicName3 === "test-topic2")
      assert(partition3 === 3 + i)
      assert(errorCode3 === 2)
      assert(highwaterMarkOffset3 === 2 + i)
      assert(offset3 === 1)
      assert(new String(getMessageValue(frame3), "UTF-8") === "world")

      // 3rd message in the response
      val MessageFrame(
          correlationId4,
          topicName4, partition4,
          errorCode4, highwaterMarkOffset4, offset4, frame4) =
        embedder.poll().asInstanceOf[MessageFrame]
      assert(correlationId4 === i)
      assert(topicName4 === "test-topic1")
      assert(partition4 === 1 + i)
      assert(errorCode4 === 1)
      assert(highwaterMarkOffset4 === 1 + i)
      assert(offset4 === 0)
      assert(new String(getMessageValue(frame4), "UTF-8") === "welcome")

      // 4th message in the response
      val MessageFrame(
          correlationId5,
          topicName5, partition5,
          errorCode5, highwaterMarkOffset5, offset5, frame5) =
        embedder.poll().asInstanceOf[MessageFrame]
      assert(correlationId5 === i)
      assert(topicName5 === "test-topic1")
      assert(partition5 === 2 + i)
      assert(errorCode5 === 1)
      assert(highwaterMarkOffset5 === 1 + i)
      assert(offset5 === 0)
      assert(new String(getMessageValue(frame5), "UTF-8") === "welcome")
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
    val msgs1 = Array[KafkaMessage](
      new KafkaMessage("welcome".getBytes("UTF-8"))
    )
    val msgs2 = Array[KafkaMessage](
      new KafkaMessage("hello".getBytes("UTF-8")),
      new KafkaMessage("world".getBytes("UTF-8"))
    )

    val data1 = KafkaFetchResponsePartitionData(1, 1 + i, new ByteBufferMessageSet(msgs1:_*))
    val data2 = KafkaFetchResponsePartitionData(2, 2 + i, new ByteBufferMessageSet(msgs2:_*))
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
