package okapies.finagle.kafka.protocol

import java.io.{DataOutputStream, InputStream, OutputStream}
import java.util.zip.{GZIPInputStream, GZIPOutputStream}

import org.jboss.netty.buffer._
import org.xerial.snappy.{SnappyInputStream, SnappyOutputStream}

sealed trait Compression {
  def id: Int

  def compress(msgs: Seq[Message]): Message
  def decompress(msg: Message): Seq[MessageWithOffset]
}

case object NoCompression extends Compression {
  final val id = 0x00

  def compress(msgs: Seq[Message]) = Compression.compress(this, msgs)
  def decompress(msg: Message) = Compression.decompress(this, msg)
}

case object GZIPCompression extends Compression {
  final val id = 0x01

  def compress(msgs: Seq[Message]) = Compression.compress(this, msgs)
  def decompress(msg: Message) = Compression.decompress(this, msg)
}

case object SnappyCompression extends Compression {
  final val id = 0x02

  def compress(msgs: Seq[Message]) = Compression.compress(this, msgs)
  def decompress(msg: Message) = Compression.decompress(this, msg)
}

object Compression {

  import Spec._

  private[this] final val WriteBufferSize = 8192

  def apply(codec: Int): Compression = codec match {
    case NoCompression.id => NoCompression
    case GZIPCompression.id => GZIPCompression
    case SnappyCompression.id => SnappyCompression
    case _ => throw new IllegalArgumentException(s"Unknown codec: $codec")
  }

  private[protocol] def compress(compression: Compression,
                                 msgs: Seq[Message]): Message = {
    val compressed = ChannelBuffers.dynamicBuffer(estimateSize(msgs))
    val out = new DataOutputStream(
      codecStream(compression, new ChannelBufferOutputStream(compressed)))
    try {
      msgs.foldLeft(0) { case (offset, msg) =>
        val buf = msg.underlying
        val size = buf.readableBytes // == msg.size
        out.writeLong(offset) // Offset => int64; TODO: Is it compatible behavior?
        out.writeInt(size)    // MessageSize => int32
        if (buf.hasArray) {   // Message
          out.write(buf.array, buf.arrayOffset, size)
        } else {
          val array = new Array[Byte](size)
          buf.readBytes(array)
          out.write(array, 0, array.length)
        }

        offset + 1
      }
    } finally {
      out.close()
    }

    Message.create(value = compressed, key = None, compression = compression)
  }

  private[protocol] def codecStream(compression: Compression,
                                    out: OutputStream): OutputStream = compression match {
    case NoCompression => out
    case GZIPCompression => new GZIPOutputStream(out)
    case SnappyCompression => new SnappyOutputStream(out)
  }

  private[this] def estimateSize(msgs: Seq[Message]): Int =
    msgs.foldLeft(0)(_ + OffsetLength + MessageSizeLength + _.size)

  private[protocol] def decompress(compression: Compression,
                                   msg: Message): Seq[MessageWithOffset] = {
    val decompressed = ChannelBuffers.dynamicBuffer
    val in = codecStream(compression, new ChannelBufferInputStream(msg.value))
    try {
      while (in.available > 0) {
        decompressed.writeBytes(in, WriteBufferSize)
      }
    } finally {
      in.close()
    }

    decompressed.decodeMessageSetElements()
  }

  private[protocol] def codecStream(compression: Compression,
                                    in: InputStream): InputStream = compression match {
    case NoCompression => in
    case GZIPCompression => new GZIPInputStream(in)
    case SnappyCompression => new SnappyInputStream(in)
  }

}
