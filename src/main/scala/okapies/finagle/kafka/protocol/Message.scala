package okapies.finagle.kafka.protocol

import java.util.zip.CRC32

import scala.math.max

import org.jboss.netty.buffer.{ChannelBuffer, ChannelBuffers}

/**
 * A message.
 *
 * {{{
 * Message => Crc MagicByte Attributes Key Value
 *   Crc => int32
 *   MagicByte => int8
 *   Attributes => int8
 *   Key => bytes
 *   Value => bytes
 * }}}
 *
 * @param _underlying the underlying buffer of the message
 */
class Message(private[this] val _underlying: ChannelBuffer) {

  import kafka.message.Message._

  def size = _underlying.readableBytes

  def crc: Long = _underlying.getInt(CrcOffset) & 0xffffffffL

  def magicByte: Byte = _underlying.getByte(MagicOffset)

  def attributes: Byte = _underlying.getByte(AttributesOffset)

  def key: Option[ChannelBuffer] = _underlying.getInt(KeySizeOffset) match {
    case keySize if keySize >= 0 => Some(_underlying.slice(KeyOffset, keySize))
    case _ => None
  }

  def value: ChannelBuffer = {
    val keySize = _underlying.getInt(KeySizeOffset)
    val valueSizeOffset = KeyOffset + max(0, keySize)
    val valueSize = _underlying.getInt(valueSizeOffset)
    val valueOffset = valueSizeOffset + ValueSizeLength

    _underlying.slice(valueOffset, valueSize)
  }

  /**
   * Creates a view of the underlying buffer with an independent readerIndex.
   */
  def underlying: ChannelBuffer = _underlying.duplicate()

  override def toString = "Message(size=%d, crc=%d, magicByte=%d, attributes=%d)"
    .format(size, crc, magicByte, attributes)

  override def equals(obj: Any): Boolean = obj match {
    case that: Message => this.hashCode == that.hashCode
    case _ => false
  }

  override def hashCode: Int = _underlying.hashCode

}

object Message {

  import kafka.message.Message._
  import Spec._

  def apply(buffer: ChannelBuffer) = new Message(buffer)

  def create(
    value: ChannelBuffer,
    key: Option[ChannelBuffer] = None,
    attributes: Byte = 0): Message = {

    val buf = ChannelBuffers.dynamicBuffer() // TODO: estimatedLength

    // Message => Crc MagicByte Attributes Key Value
    buf.writerIndex(MagicOffset) // skip the Crc field
    buf.encodeInt8(CurrentMagicValue)
    buf.encodeInt8(attributes)
    key match {
      case Some(b) => buf.encodeBytes(b)
      case None => buf.encodeBytes(null: ChannelBuffer)
    }
    buf.encodeBytes(value)

    // set Crc
    val len = buf.readableBytes
    buf.setInt(CrcOffset, computeCRC32(buf, MagicOffset, len - MagicOffset))

    new Message(buf)
  }

  private def computeCRC32(buf: ChannelBuffer, offset: Int, length: Int): Int32 = {
    val crc = new CRC32()

    if (buf.hasArray) {
      crc.update(buf.array, buf.arrayOffset + offset, length)
    } else {
      val array = new Array[Byte](buf.readableBytes)
      buf.getBytes(buf.readerIndex, array)
      crc.update(array, offset, length)
    }

    (crc.getValue & 0xffffffffL).asInstanceOf[Int] // 32-bit
  }

}

/**
 * A message with a offset.
 */
case class MessageWithOffset(
  offset: Long,    // int64
  message: Message // Message
)
