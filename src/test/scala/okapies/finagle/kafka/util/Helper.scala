package okapies.finagle.kafka.util

import java.lang.String

import java.nio.ByteBuffer
import java.nio.charset.Charset

object Helper {

  import scala.language.implicitConversions
  import kafka.message.{
    Message => KafkaMessage,
    NoCompressionCodec
  }

  val utf8 = Charset.forName("UTF-8")

  implicit def asByteBufferOps(buf: ByteBuffer) = new ByteBufferOps(buf)

  class ByteBufferOps(buf: ByteBuffer) {
    def asString() = {
      val value = new Array[Byte](buf.limit)
      buf.get(value)

      new String(value, utf8)
    }
  }

  def kafkaMessage(bytes: Array[Byte], key: Array[Byte]) = new KafkaMessage(
    bytes      = bytes,
    key        = key,
    timestamp  = KafkaMessage.NoTimestamp,
    codec      = NoCompressionCodec,
    magicValue = KafkaMessage.CurrentMagicValue
  )

}
