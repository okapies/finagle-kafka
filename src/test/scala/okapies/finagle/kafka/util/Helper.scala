package okapies.finagle.kafka.util

import java.lang.String

import java.nio.ByteBuffer
import java.nio.charset.Charset

object Helper {

  val utf8 = Charset.forName("UTF-8")

  implicit def asByteBufferOps(buf: ByteBuffer) = new ByteBufferOps(buf)

  class ByteBufferOps(buf: ByteBuffer) {
    def asString() = {
      val value = new Array[Byte](buf.limit)
      buf.get(value)

      new String(value, utf8)
    }
  }

}
