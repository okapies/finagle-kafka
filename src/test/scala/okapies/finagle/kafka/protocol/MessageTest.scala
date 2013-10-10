package okapies.finagle.kafka
package protocol

import org.scalatest._
import org.scalatest.matchers._

import org.jboss.netty.buffer.ChannelBuffers

import util.GatheringByteChannelMock

class MessageTest extends FlatSpec with ShouldMatchers {

  import kafka.message.{
    ByteBufferMessageSet,
    Message => KafkaMessage,
    NoCompressionCodec
  }
  import util.Helper._

  behavior of "A Message"

  it should "encode a no compressed message" in {
    val msg1 = Message.create(
      ChannelBuffers.wrappedBuffer("value1".getBytes(utf8)),     // value
      Some(ChannelBuffers.wrappedBuffer("key1".getBytes(utf8))), // key
      0                                                          // codec
    )
    val kafkaMsg1 = new KafkaMessage(msg1.underlying.toByteBuffer)

    assert(kafkaMsg1.checksum === msg1.crc)
    assert(kafkaMsg1.magic === msg1.magicByte)
    assert(kafkaMsg1.attributes === msg1.attributes)
    assert(kafkaMsg1.key.asString === msg1.key.get.toString(utf8))
    assert(kafkaMsg1.payload.asString === msg1.value.toString(utf8))
  }

  it should "decode a no compressed message" in {
    val kafkaMsg1 = new KafkaMessage(
      "value1".getBytes(utf8), // value
      "key1".getBytes(utf8),   // key
      NoCompressionCodec       // codec
    )
    val msg1 = Message(ChannelBuffers.wrappedBuffer(kafkaMsg1.buffer))

    assert(msg1.crc === kafkaMsg1.checksum)
    assert(msg1.magicByte === kafkaMsg1.magic)
    assert(msg1.attributes === kafkaMsg1.attributes)
    assert(msg1.key.get.toString(utf8) === kafkaMsg1.key.asString)
    assert(msg1.value.toString(utf8) === kafkaMsg1.payload.asString)
  }

  behavior of "Spec.encodeMessageSet"

  behavior of "Spec.decodeMessageSet"

}
