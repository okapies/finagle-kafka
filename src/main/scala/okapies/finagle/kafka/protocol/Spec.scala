package okapies.finagle.kafka.protocol

import java.nio.charset.Charset

import scala.collection.mutable.ArrayBuffer

import org.jboss.netty.buffer.ChannelBuffer

import kafka.common.KafkaException

private[protocol] object Spec {

  // Type aliases
  type Int8 = Byte
  type Int16 = Short
  type Int32 = Int
  type Int64 = Long

  // Api Versions
  val ApiVersion0 = 0: Int16

  // Api Keys
  val ApiKeyProduce      = 0: Int16
  val ApiKeyFetch        = 1: Int16
  val ApiKeyOffset       = 2: Int16
  val ApiKeyMetadata     = 3: Int16
  val ApiKeyLeaderAndIsr = 4: Int16
  val ApiKeyStopReplica  = 5: Int16
  val ApiKeyOffsetCommit = 6: Int16
  val ApiKeyOffsetFetch  = 7: Int16

  // length of fields
  val ArraySizeLength = 4

  // Encoding
  val DefaultCharset = Charset.forName("UTF-8")

  /*
   * Implicit conversions
   */

  // TODO: Use implicit value class in Scala 2.10
  //import scala.language.implicitConversions

  implicit def asKafkaError(code: Int16): KafkaError = KafkaError(code)

  implicit def asKafkaChannelBuffer(buf: ChannelBuffer) = new KafkaChannelBuffer(buf)

  /**
   * The wrapper of ChannelBuffer that provides helper methods for Kafka protocol.
   */
  //private[protocol] implicit class KafkaChannelBuffer(val buf: ChannelBuffer) extends AnyVal {
  class KafkaChannelBuffer(val buf: ChannelBuffer) {

    /*
     * Methods for encoding value and writing into buffer.
     */

    /**
     * A simple wrapper for [[org.jboss.netty.buffer.ChannelBuffer#writeByte]].
     */
    @inline
    def encodeInt8(integer: Int8) = buf.writeByte(integer.asInstanceOf[Int])

    /**
     * A simple wrapper for [[org.jboss.netty.buffer.ChannelBuffer#writeShort]].
     */
    @inline
    def encodeInt16(integer: Int16) = buf.writeShort(integer.asInstanceOf[Int])

    /**
     * A simple wrapper for [[org.jboss.netty.buffer.ChannelBuffer#writeInt]].
     */
    @inline
    def encodeInt32(integer: Int32) = buf.writeInt(integer)

    /**
     * A simple wrapper for [[org.jboss.netty.buffer.ChannelBuffer#writeLong]].
     */
    @inline
    def encodeInt64(integer: Int64) = buf.writeLong(integer)

    /**
     * Write the buffer's data with prefixed length field. The number of the
     * transferred bytes is (4 + bytes.readableBytes).
     */
    @inline
    def encodeBytes(bytes: ChannelBuffer) {
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
      import kafka.message.MessageSet._

      buf.encodeInt32(messages.foldLeft(0)(_ + OffsetLength + MessageSizeLength + _.size))
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
    def decodeBytes(): ChannelBuffer = {
      val n = buf.decodeInt32() // N => int32
      buf.readBytes(n)          // content
    }

    @inline
    def decodeString(): String = {
      val n = buf.decodeInt16()    // N => int16
      val bytes = buf.readBytes(n) // content

      val array =
        if (bytes.hasArray) {
          bytes.array
        } else {
          val _array = new Array[Byte](n)
          bytes.readBytes(_array)
          _array
        }

      new String(array, bytes.arrayOffset, n, DefaultCharset)
    }

    @inline
    def decodeArray[A](f: => A): Seq[A] = {
      val n: Int32 = buf.readInt() // N => int32
      (0 until n).map(_ => f)
    }

    @inline
    def decodeStringArray() = decodeArray(buf.decodeString())

    @inline
    def decodeMessageSet(): Seq[MessageWithOffset] = {
      val size = buf.decodeInt32()
      val messageSetBuf = buf.readBytes(size)

      val messages = ArrayBuffer[MessageWithOffset]()
      while(messageSetBuf.readableBytes > 0) {
        val offset = messageSetBuf.decodeInt64()
        val bytes = messageSetBuf.decodeBytes()
        messages.append(MessageWithOffset(offset, Message(bytes)))
      }

      messages
    }

  }

}
