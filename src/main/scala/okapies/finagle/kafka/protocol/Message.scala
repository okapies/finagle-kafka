package okapies.finagle.kafka.protocol

import java.util.zip.CRC32

import scala.math.max

import io.netty.buffer.{ByteBuf, Unpooled}

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
class Message(private[this] val _underlying: ByteBuf) {

  import Message._

  def size = _underlying.readableBytes

  def crc: Long = _underlying.getInt(CrcOffset) & 0xffffffffL

  def magicByte: Byte = _underlying.getByte(MagicOffset)

  def attributes: Byte = _underlying.getByte(AttributesOffset)

  def key: Option[ByteBuf] = _underlying.getInt(KeySizeOffset) match {
    case keySize if keySize >= 0 => Some(_underlying.slice(KeyOffset, keySize))
    case _ => None
  }

  def value: ByteBuf = {
    val keySize = _underlying.getInt(KeySizeOffset)
    val valueSizeOffset = KeyOffset + max(0, keySize)
    val valueSize = _underlying.getInt(valueSizeOffset)
    val valueOffset = valueSizeOffset + ValueSizeLength

    _underlying.slice(valueOffset, valueSize)
  }

  /**
   * Creates a view of the underlying buffer with an independent readerIndex.
   */
  def underlying: ByteBuf = _underlying.duplicate()

  override def toString = "Message(size=%d, crc=%d, magicByte=%d, attributes=%d)"
    .format(size, crc, magicByte, attributes)

  override def equals(obj: Any): Boolean = obj match {
    case that: Message => this.hashCode == that.hashCode
    case _ => false
  }

  override def hashCode: Int = _underlying.hashCode

}

object Message {

  import Spec._

  // The offset and length of the fields
  final val CrcOffset = 0
  final val CrcLength = 4
  final val MagicOffset = CrcOffset + CrcLength
  final val MagicLength = 1
  final val AttributesOffset = MagicOffset + MagicLength
  final val AttributesLength = 1
  final val KeySizeOffset = AttributesOffset + AttributesLength
  final val KeySizeLength = 4
  final val KeyOffset = KeySizeOffset + KeySizeLength
  final val ValueSizeLength = 4

  final val MinHeaderSize =
    CrcLength + MagicLength + AttributesLength + KeySizeLength + ValueSizeLength

  // Magic value
  final val CurrentMagicValue = 0: Int8

  def apply(buffer: ByteBuf) = new Message(buffer)

  def create(
    value: ByteBuf,
    key: Option[ByteBuf] = None,
    attributes: Byte = 0): Message = {

    val buf = Unpooled.buffer() // TODO: estimatedLength

    // Message => Crc MagicByte Attributes Key Value
    buf.writerIndex(MagicOffset) // skip the Crc field
    buf.encodeInt8(CurrentMagicValue)
    buf.encodeInt8(attributes)
    key match {
      case Some(b) => buf.encodeBytes(b)
      case None => buf.encodeBytes(null: ByteBuf)
    }
    buf.encodeBytes(value)

    // set Crc
    val len = buf.readableBytes
    buf.setInt(CrcOffset, computeCRC32(buf, MagicOffset, len - MagicOffset))

    new Message(buf)
  }

  private def computeCRC32(buf: ByteBuf, offset: Int, length: Int): Int32 = {
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
