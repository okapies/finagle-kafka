package okapies.finagle.kafka
package protocol

import org.scalatest._
import org.scalatest.matchers._

import org.jboss.netty.buffer.ChannelBuffers

class MessageTest extends FlatSpec with Matchers {

  import kafka.message.{
    Message => KafkaMessage,
    NoCompressionCodec
  }
  import util.Helper._

  behavior of "A Message"

  it should "encode a no compressed message" in {
    val msg1 = Message.create(
      ChannelBuffers.wrappedBuffer("value1".getBytes(utf8)),     // value
      Some(ChannelBuffers.wrappedBuffer("key1".getBytes(utf8))), // key
      NoCompression                                              // codec
    )
    val kafkaMsg1 = new KafkaMessage(msg1.underlying.toByteBuffer)

    assert(kafkaMsg1.checksum === msg1.crc)
    assert(kafkaMsg1.magic === msg1.magicByte)
    assert(kafkaMsg1.attributes === msg1.attributes)
    assert(kafkaMsg1.key.asString === "key1")
    assert(kafkaMsg1.payload.asString === "value1")

    val msg2 = Message.create(
      ChannelBuffers.wrappedBuffer("value2".getBytes(utf8)),     // value
      None,                                                      // key
      NoCompression                                              // codec
    )
    val kafkaMsg2 = new KafkaMessage(msg2.underlying.toByteBuffer)

    assert(kafkaMsg2.checksum === msg2.crc)
    assert(kafkaMsg2.magic === msg2.magicByte)
    assert(kafkaMsg2.attributes === msg2.attributes)
    assert(kafkaMsg2.key === null)
    assert(kafkaMsg2.payload.asString === "value2")
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
    assert(msg1.key.get.toString(utf8) === "key1")
    assert(msg1.value.toString(utf8) === "value1")

    val kafkaMsg2 = new KafkaMessage(
      "value2".getBytes(utf8), // value
      NoCompressionCodec       // codec
    )
    val msg2 = Message(ChannelBuffers.wrappedBuffer(kafkaMsg2.buffer))

    assert(msg2.crc === kafkaMsg2.checksum)
    assert(msg2.magicByte === kafkaMsg2.magic)
    assert(msg2.attributes === kafkaMsg2.attributes)
    assert(msg2.key === None)
    assert(msg2.value.toString(utf8) === "value2")
  }

}
