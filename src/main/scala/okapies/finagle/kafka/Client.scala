package okapies.finagle.kafka

import com.twitter.finagle.builder.ClientBuilder
import okapies.finagle.kafka.protocol.Kafka

object Client {

  def apply(hosts: String) = ClientBuilder()
    .codec(Kafka())
    .hosts(hosts)
    .hostConnectionLimit(1)
    .build()

}
