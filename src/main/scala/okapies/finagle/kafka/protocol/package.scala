package okapies.finagle.kafka

import org.jboss.netty.buffer.ChannelBuffers

package object protocol {

  /*
   * Implicit conversions
   */
  import scala.language.implicitConversions

  implicit def asRequiredAcks(requiredAcks: Short) = RequiredAcks(requiredAcks)

  implicit def asMessage(value: String): Message =
    Message.create(ChannelBuffers.wrappedBuffer(value.getBytes(Spec.DefaultCharset)))

  implicit def asMessage(entry: (String, String)): Message =
    Message.create(
      ChannelBuffers.wrappedBuffer(entry._2.getBytes(Spec.DefaultCharset)),
      Option(ChannelBuffers.wrappedBuffer(entry._1.getBytes(Spec.DefaultCharset))))

  implicit def asKafkaError(code: Short): KafkaError = KafkaError(code)

}
