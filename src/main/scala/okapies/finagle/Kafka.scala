package okapies.finagle

import com.twitter.finagle.{Client, Name}
import com.twitter.finagle.client.{Bridge, DefaultClient}
import com.twitter.finagle.dispatch.PipeliningDispatcher
import com.twitter.finagle.netty3.Netty3Transporter
import com.twitter.finagle.pool.SingletonPool
import com.twitter.finagle.stats.StatsReceiver

import okapies.finagle.kafka.protocol.{KafkaBatchClientPipelineFactory, KafkaStreamClientPipelineFactory, Request, Response}

trait KafkaRichClient { self: Client[Request, Response] =>

  def newRichClient(dest: String): kafka.Client = kafka.Client(newService(dest))

  def newRichClient(dest: Name, label: String): kafka.Client = kafka.Client(newService(dest, label))

}

object KafkaTransporter extends Netty3Transporter[Request, Response](
  name = "kafka",
  pipelineFactory = KafkaBatchClientPipelineFactory
)

object KafkaClient extends DefaultClient[Request, Response](
  name = "kafka",
  endpointer =
    Bridge[Request, Response, Request, Response](KafkaTransporter, new PipeliningDispatcher(_)),
  pool = (sr: StatsReceiver) => new SingletonPool(_, sr)
) with KafkaRichClient

object Kafka extends Client[Request, Response] with KafkaRichClient {

  def newClient(dest: Name, label: String) = KafkaClient.newClient(dest, label)

}
