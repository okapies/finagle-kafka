package okapies.finagle.kafka

//import scala.language.implicitConversions

import org.jboss.netty.buffer.ChannelBuffers

package object protocol {

  /*
   * Implicit conversions
   */

  implicit def asRequiredAcks(requiredAcks: Short) = RequiredAcks(requiredAcks)

  implicit def asMessage(value: String): Message =
    Message.create(ChannelBuffers.wrappedBuffer(value.getBytes(Spec.DefaultCharset)))

  implicit def asKafkaError(code: Short): KafkaError = KafkaError(code)

}
