package okapies.finagle.kafka

import io.netty.buffer.Unpooled

package object protocol {

  /*
   * Implicit conversions
   */
  import scala.language.implicitConversions

  implicit def asRequiredAcks(requiredAcks: Short) = RequiredAcks(requiredAcks)

  implicit def asMessage(value: String): Message =
    Message.create(Unpooled.wrappedBuffer(value.getBytes(Spec.DefaultCharset)))

  implicit def asMessage(entry: (String, String)): Message =
    Message.create(
      Unpooled.wrappedBuffer(entry._2.getBytes(Spec.DefaultCharset)),
      Option(Unpooled.wrappedBuffer(entry._1.getBytes(Spec.DefaultCharset))))

  implicit def asKafkaError(code: Short): KafkaError = KafkaError(code)

}
