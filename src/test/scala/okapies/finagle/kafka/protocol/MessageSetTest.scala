package okapies.finagle.kafka
package protocol

import org.scalatest._
import org.scalatest.matchers._

import java.nio.ByteBuffer
import java.util.concurrent.atomic.AtomicLong

import io.netty.buffer.Unpooled

class MessageSetTest extends FlatSpec with Matchers {

  import kafka.message.{
    ByteBufferMessageSet,
    Message => KafkaMessage,
    NoCompressionCodec
  }
  import Spec._
  import util.Helper._

  behavior of "encodeMessageSet()"

  it should "encode a MessageSet to bytes" in {
    val msgs1 = Seq[Message](
      Message.create(
        Unpooled.wrappedBuffer("value1".getBytes(utf8)),
        Some(Unpooled.wrappedBuffer("key1".getBytes(utf8))),
        0
      ),
      Message.create(
        Unpooled.wrappedBuffer("value2".getBytes(utf8)),
        Some(Unpooled.wrappedBuffer("key2".getBytes(utf8))),
        0
      )
    )
    val buf1 = Unpooled.buffer()
    var i: Long = 1
    buf1.encodeMessageSet(msgs1) { message =>
      buf1.encodeInt64(i) // offset
      buf1.encodeBytes(message.underlying)
      i += 1
    }

    val size1 = buf1.readableBytes
    val kafkaMsgs1 =
      new ByteBufferMessageSet(buf1.nioBuffer(4, size1 - 4)) // skip MessageSetSize
    val it1 = kafkaMsgs1.iterator

    val msg1 = it1.next()
    assert(msg1.offset === 1)
    assert(msg1.message.key.asString === "key1")
    assert(msg1.message.payload.asString === "value1")

    val msg2 = it1.next()
    assert(msg2.offset === 2)
    assert(msg2.message.key.asString === "key2")
    assert(msg2.message.payload.asString === "value2")

    assert(it1.hasNext === false)
  }

  behavior of "decodeMessageSet()"

  it should "decode bytes into a MessageSet" in {
    val kafkaMsgs1 = new ByteBufferMessageSet(
      NoCompressionCodec,
      new AtomicLong(1),
      new KafkaMessage("value1".getBytes(utf8), "key1".getBytes(utf8)),
      new KafkaMessage("value2".getBytes(utf8), "key2".getBytes(utf8))
    )
    val size1 = kafkaMsgs1.sizeInBytes
    val buf1 = ByteBuffer.allocateDirect(4 /* Size */ + size1)
    buf1.putInt(size1)
    buf1.put(kafkaMsgs1.buffer)
    buf1.rewind()

    val chBuf1 = Unpooled.wrappedBuffer(buf1)
    val msgs1 = chBuf1.decodeMessageSet()
    assert(chBuf1.readableBytes === 0)

    val msg11 = msgs1(0)
    assert(msg11.offset === 1)
    assert(msg11.message.key.get.toString(utf8) === "key1")
    assert(msg11.message.value.toString(utf8) === "value1")

    val msg12 = msgs1(1)
    assert(msg12.offset === 2)
    assert(msg12.message.key.get.toString(utf8) === "key2")
    assert(msg12.message.value.toString(utf8) === "value2")
  }

  it should "decode bytes including a partial message into a MessageSet" in {
    val kafkaMsgs1 = new ByteBufferMessageSet(
      NoCompressionCodec,
      new AtomicLong(1),
      new KafkaMessage("value1".getBytes(utf8), "key1".getBytes(utf8)),
      new KafkaMessage("value2".getBytes(utf8), "key2".getBytes(utf8))
    )
    val size1 = kafkaMsgs1.sizeInBytes - 5 // make 2nd message partial
    val buf1 = ByteBuffer.allocateDirect(4 /* Size */ + size1)
    buf1.putInt(size1)
    buf1.put(kafkaMsgs1.buffer.array, 0, size1)
    buf1.rewind()

    val chBuf1 = Unpooled.wrappedBuffer(buf1)
    val msgs1 = chBuf1.decodeMessageSet()
    assert(chBuf1.readableBytes === 0)

    val msg11 = msgs1(0)
    assert(msg11.offset === 1)
    assert(msg11.message.key.get.toString(utf8) === "key1")
    assert(msg11.message.value.toString(utf8) === "value1")

    // 2nd message must be ignored
    assert(msgs1.length === 1)
  }

}
