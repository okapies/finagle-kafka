package okapies.finagle.kafka
package protocol

import java.nio.ByteBuffer

import scala.collection.immutable.ListMap

import io.netty.buffer.{ByteBuf, Unpooled}
import io.netty.channel.embedded.EmbeddedChannel

import okapies.finagle.kafka.util.GatheringByteChannelMock

import org.scalatest._
import org.scalatest.matchers._

class StreamFrameDecoderTest extends FlatSpec with Matchers {

  import kafka.api.{
    FetchResponse => KafkaFetchResponse,
    FetchResponsePartitionData => KafkaFetchResponsePartitionData,
    FetchResponseSend => KafkaFetchResponseSend,
    ProducerResponse => KafkaProducerResponse,
    ProducerResponseStatus => KafkaProducerResponseStatus
  }
  import _root_.kafka.common.TopicAndPartition
  import _root_.kafka.message.{ByteBufferMessageSet, Message => KafkaMessage}
  import Spec._
  import util.Helper._

  behavior of "A StreamFrameDecoder"

  it should "decode the received ProduceResponse into a ResponseFrame" in {
    val channel = new EmbeddedChannel(
      new StreamFrameDecoder(RequestCorrelator(_ => Some(ApiKeyProduce)), 1024)
    )
    val headerLength = 4 /* Size */ + 4 /* CorrelationId */

    // do multiple times to detect state init failure
    for (i <- 0 to 1) {
      val buf1 = createProduceResponseAsByteBuf(i)
      channel.writeInbound(Unpooled.copiedBuffer(buf1))
      val BufferResponseFrame(apiKey1, correlationId1, frame1) = channel.readInbound[BufferResponseFrame]()

      assert(apiKey1 === ApiKeyProduce)

      assert(correlationId1 === i)
      assert(frame1 ===
        buf1.slice(buf1.readerIndex + headerLength, buf1.readableBytes - headerLength))
    }
  }

  it should "decode the received FetchResponse into a ResponseFrame" in {
    val channel = new EmbeddedChannel(
      new StreamFrameDecoder(RequestCorrelator(_ => Some(ApiKeyFetch)), 1024)
    )

    // do multiple times to detect state init failure
    for (i <- 0 to 1) {
      val buf1 = createFetchResponseAsByteBuf(i)
      channel.writeInbound(buf1.duplicate())
      val FetchResponseFrame(correlationId1) = channel.readInbound[FetchResponseFrame]()

      assert(correlationId1 === i)

      // 1st partition in the response
      val PartitionStatus(topicName1, partition1, error1, highwaterMarkOffset1) = channel.readInbound[PartitionStatus]
      assert(topicName1 === "test-topic2")
      assert(partition1 === 3 + i)
      assert(error1.code === 2)
      assert(highwaterMarkOffset1 === 2 + i)

      // 1st message in the response
      val FetchedMessage(topicName11, partition11, offset11, msg11) = channel.readInbound[FetchedMessage]()
      assert(topicName11 === "test-topic2")
      assert(partition11 === 3 + i)
      assert(offset11 === 0)
      assert(new String(getMessageValue(msg11), utf8) === "hello")

      // 2nd message in the response
      val FetchedMessage(topicName12, partition12, offset12, msg12) = channel.readInbound[FetchedMessage]()
      assert(topicName12 === "test-topic2")
      assert(partition12 === 3 + i)
      assert(offset12 === 1)
      assert(new String(getMessageValue(msg12), utf8) === "world")

      // 2nd partition in the response
      val PartitionStatus(topicName2, partition2, error2, highwaterMarkOffset2) = channel.readInbound[PartitionStatus]()
      assert(topicName2 === "test-topic1")
      assert(partition2 === 1 + i)
      assert(error2.code === 1)
      assert(highwaterMarkOffset2 === 1 + i)

      // 3rd message in the response
      val FetchedMessage(topicName21, partition21, offset21, msg21) = channel.readInbound[FetchedMessage]()
      assert(topicName21 === "test-topic1")
      assert(partition21 === 1 + i)
      assert(offset21 === 0)
      assert(new String(getMessageValue(msg21), utf8) === "welcome")

      // 3rd partition in the response
      val PartitionStatus(topicName3, partition3, error3, highwaterMarkOffset3) = channel.readInbound[PartitionStatus]()
      assert(topicName3 === "test-topic1")
      assert(partition3 === 2 + i)
      assert(error3.code === 1)
      assert(highwaterMarkOffset3 === 1 + i)

      // 4th message in the response
      val FetchedMessage(topicName31, partition31, offset31, msg31) = channel.readInbound[FetchedMessage]()
      assert(topicName31 === "test-topic1")
      assert(partition31 === 2 + i)
      assert(offset31 === 0)
      assert(new String(getMessageValue(msg31), utf8) === "welcome")


      val msg6 = channel.readInbound[ResponseFrame]()
      assert(msg6 === NilMessageFrame)
    }
  }

  private[this] def getMessageValue(msg: Message): Array[Byte] = {
    val payload = new KafkaMessage(msg.underlying.nioBuffer).payload
    val value = new Array[Byte](payload.limit)
    payload.get(value)

    value
  }

  private[this] def createProduceResponseAsByteBuf(i: Int) = {
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

    Unpooled.wrappedBuffer(buf)
  }

  private[this] def createFetchResponseAsByteBuf(i: Int) = {
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

    Unpooled.wrappedBuffer(buf)
  }

}
