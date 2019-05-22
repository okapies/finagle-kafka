package okapies.finagle.kafka.protocol

import java.nio.charset.Charset

import scala.annotation.tailrec
import scala.collection.mutable.ArrayBuffer

import io.netty.buffer.ByteBuf

import _root_.kafka.common.KafkaException
import _root_.kafka.message.InvalidMessageException

private[protocol] object Spec {

  // Type aliases
  type Int8 = Byte
  type Int16 = Short
  type Int32 = Int
  type Int64 = Long

  // Api Versions
  final val ApiVersion0 = 0: Int16

  // Api Keys
  final val ApiKeyProduce          = 0: Int16
  final val ApiKeyFetch            = 1: Int16
  final val ApiKeyOffset           = 2: Int16
  final val ApiKeyMetadata         = 3: Int16
  final val ApiKeyLeaderAndIsr     = 4: Int16
  final val ApiKeyStopReplica      = 5: Int16
  final val ApiKeyOffsetCommit     = 8: Int16
  final val ApiKeyOffsetFetch      = 9: Int16
  final val ApiKeyConsumerMetadata = 10: Int16

  // length of fields
  final val CorrelationIdLength = 4
  final val ArraySizeLength = 4

  final val OffsetLength = 8
  final val MessageSizeLength = 4

  // Encoding
  final val DefaultCharset = Charset.forName("UTF-8")

  /*
   * Implicit conversions
   */

  import scala.language.implicitConversions

  /**
   * The wrapper of ByteBuf that provides helper methods for Kafka protocol.
   */
  private[protocol] implicit class KafkaByteBuf(val buf: ByteBuf) extends AnyVal {

    /*
     * Methods for encoding value and writing into buffer.
     */

    /**
     * A simple wrapper for [[org.jboss.netty.buffer.ByteBuf#writeByte]].
     */
    @inline
    def encodeInt8(integer: Int8) = buf.writeByte(integer.asInstanceOf[Int])

    /**
     * A simple wrapper for [[org.jboss.netty.buffer.ByteBuf#writeShort]].
     */
    @inline
    def encodeInt16(integer: Int16) = buf.writeShort(integer.asInstanceOf[Int])

    /**
     * A simple wrapper for [[org.jboss.netty.buffer.ByteBuf#writeInt]].
     */
    @inline
    def encodeInt32(integer: Int32) = buf.writeInt(integer)

    /**
     * A simple wrapper for [[org.jboss.netty.buffer.ByteBuf#writeLong]].
     */
    @inline
    def encodeInt64(integer: Int64) = buf.writeLong(integer)

    /**
     * Write the buffer's data with prefixed length field. The number of the
     * transferred bytes is (4 + bytes.readableBytes).
     */
    @inline
    def encodeBytes(bytes: ByteBuf) {
      if (bytes != null) {
        val n: Int32 = bytes.readableBytes
        buf.encodeInt32(n)          // int32 (length field)
        buf.writeBytes(bytes, 0, n) // content
      } else {
        buf.encodeInt32(-1:Int32) // indicates null
      }
    }

    /**
     * Write the array's data with prefixed length field. The number of the
     * transferred bytes is (4 + bytes.length).
     */
    @inline
    def encodeBytes(bytes: Array[Byte]) {
      if (bytes != null) {
        val n: Int32 = bytes.length
        buf.encodeInt32(n)          // int32 (length field)
        buf.writeBytes(bytes, 0, n) // content
      } else {
        buf.encodeInt32(-1:Int32) // indicates null
      }
    }

    /**
     * Write the string as UTF-8 decoded bytes, with prefixed length field.
     * The number of the transferred bytes is (2 + str.getBytes(UTF-8).length).
     */
    @inline
    def encodeString(str: String) {
      if (str != null) {
        val bytes = str.getBytes(DefaultCharset)
        val n = bytes.length
        if ( n <= Short.MaxValue) {
          buf.encodeInt16(n.asInstanceOf[Int16]) // int16 (length field)
          buf.writeBytes(bytes, 0, n)            // content
        } else {
          throw new KafkaException("Size of string exceeds " + Short.MaxValue + ".")
        }
      } else {
        buf.encodeInt16(-1:Int16) // indicates null
      }
    }

    /**
     * Write the array with prefixed length field.
     */
    @inline
    def encodeArray[A](array: Seq[A])(f: A => Unit) {
      if (array != null) {
        buf.encodeInt32(array.length) // N => int32  (length field)
        array.foreach(f)              // write each element
      } else {
        buf.encodeInt32(-1:Int32) // what is null indicator?
      }
    }

    /**
     * Write the string array with prefixed length field.
     */
    @inline
    def encodeStringArray(array: Seq[String]) = encodeArray(array)(buf.encodeString)

    /**
     * Write the map as array with prefixed length field.
     */
    @inline
    def encodeArray[A, B](array: Map[A, B])(f: ((A, B)) => Unit) {
      if (array != null) {
        buf.encodeInt32(array.size) // N => int32  (length field)
        array.foreach(f)            // write each element
      } else {
        buf.encodeInt32(-1:Int32) // what is null indicator?
      }
    }

    /**
     * Write the MessageSet. MessageSet is NOT preceded by an int32 like other array elements.
     */
    @inline
    def encodeMessageSet(messages: Seq[Message])(f: Message => Unit) = {
      buf.encodeInt32(messages.foldLeft(0)(_ + OffsetLength + MessageSizeLength + _.size))
      messages.foreach(f)
    }

    @inline
    def encodeMessageSetWithOffset(messages: Seq[MessageWithOffset])(f: MessageWithOffset => Unit) = {
      buf.encodeInt32(messages.foldLeft(0)(_ + OffsetLength + MessageSizeLength + _.message.size))
      messages.foreach(f)
    }


    /*
     * Methods for decoding next bytes.
     */

    @inline
    def decodeInt8(): Int8 = buf.readByte()

    @inline
    def decodeInt16(): Int16 = buf.readShort()

    @inline
    def decodeInt32(): Int32 = buf.readInt()

    @inline
    def decodeInt64(): Int64 = buf.readLong()

    @inline
    def decodeBytes(): ByteBuf = {
      val n = buf.decodeInt32() // N => int32
      buf.readBytes(n)          // content
    }

    @inline
    def decodeString(): String = {
      val n = buf.decodeInt16()    // N => int16
      val bytes = buf.readBytes(n) // content

      val (array, offset) =
        if (bytes.hasArray) {
          (bytes.array, bytes.arrayOffset)
        } else {
          val _array = new Array[Byte](n)
          bytes.readBytes(_array)
          (_array, 0)
        }

      new String(array, offset, n, DefaultCharset)
    }

    @inline
    def decodeArray[A](f: => A): Seq[A] = {
      val n: Int32 = buf.decodeInt32() // N => int32
      (0 until n).map(_ => f)
    }

    @inline
    def decodeStringArray() = decodeArray(buf.decodeString())

    @inline
    def decodeMessageSet(): Seq[MessageWithOffset] = {
      @tailrec
      def decodeMessages(msgSetBuf: ByteBuf,
                         msgSet: ArrayBuffer[MessageWithOffset]): Seq[MessageWithOffset] = {
        if (msgSetBuf.readableBytes < 12) {    // 12 = length(Offset+MessageSize)
          msgSet
        } else {
          val offset = msgSetBuf.decodeInt64() // Offset => int64
          val size = msgSetBuf.decodeInt32()   // MessageSize => int32
          if (size < Message.MinHeaderSize) {
            throw new InvalidMessageException(s"Message size is corrupted: $size")
          }

          if (msgSetBuf.readableBytes < size) {
            // ignore a partial message at the end of the message set
            msgSet
          } else {
            val bytes = msgSetBuf.readBytes(size) // Message
            msgSet.append(MessageWithOffset(offset, Message(bytes)))

            decodeMessages(msgSetBuf, msgSet)
          }
        }
      }

      val size = buf.decodeInt32()        // MessageSetSize => int32
      val msgSetBuf = buf.readBytes(size) // MessageSet

      decodeMessages(msgSetBuf, ArrayBuffer[MessageWithOffset]())
    }

  }

}
