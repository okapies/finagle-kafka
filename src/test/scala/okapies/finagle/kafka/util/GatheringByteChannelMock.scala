package okapies.finagle.kafka.util

import java.nio.channels.GatheringByteChannel
import java.nio.ByteBuffer

class GatheringByteChannelMock(buffer: ByteBuffer) extends GatheringByteChannel {

  def isOpen: Boolean = true

  def close() { }

  def write(src: ByteBuffer): Int = {
    buffer.put(src)
    src.limit
  }

  def write(srcs: Array[ByteBuffer]): Long = srcs.map(write).sum

  def write(srcs: Array[ByteBuffer], offset: Int, length: Int): Long =
    srcs.slice(offset, offset + length).map(write).sum

}
