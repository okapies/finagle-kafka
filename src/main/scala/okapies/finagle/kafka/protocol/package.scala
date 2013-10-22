package okapies.finagle.kafka

import org.jboss.netty.buffer.ChannelBuffers

package object protocol {

  implicit def asMessage(value: String): Message =
    Message.create(ChannelBuffers.wrappedBuffer(value.getBytes(Spec.DefaultCharset)))

}
