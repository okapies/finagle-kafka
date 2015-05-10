package okapies.finagle

import com.twitter.util.{Future, Promise}
import com.twitter.finagle.{Client, Name, Server, Service, ServiceFactory}
import com.twitter.finagle.client.{Bridge, DefaultClient}
import com.twitter.finagle.server.DefaultServer
import com.twitter.finagle.dispatch.{PipeliningDispatcher, GenSerialServerDispatcher}
import com.twitter.finagle.netty3.{Netty3Transporter, Netty3Listener}
import com.twitter.finagle.pool.SingletonPool
import com.twitter.finagle.transport.Transport
import com.twitter.finagle.stats.StatsReceiver
import java.net.SocketAddress

import okapies.finagle.kafka.protocol._

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

class KafkaServerDispatcher(
  trans: Transport[Response, Request],
  service: Service[Request, Response])
extends GenSerialServerDispatcher[Request, Response, Response, Request](trans) {

  trans.onClose ensure {
    service.close()
  }

  protected def dispatch(msg: Request, eos: Promise[Unit]) = msg match {
    case req: Request =>
      service(req) ensure eos.setDone()
    case failure =>
      eos.setDone
      Future.exception(new IllegalArgumentException(s"Invalid message $failure"))
  }

  protected def handle(resp: Response) = resp match {
    case r:NilResponse => Future.Unit
    case anyResp => trans.write(resp)
  }
}


object Kafka
extends Client[Request, Response]
with KafkaRichClient
with Server[Request, Response] {

  def newClient(dest: Name, label: String) = KafkaClient.newClient(dest, label)

  class Server() extends DefaultServer[Request, Response, Response, Request](
    "kafka-server",
    new Netty3Listener(
      "kafka-server-listener",
      new KafkaServerPipelineFactory
    ),
    new KafkaServerDispatcher(_, _)
  )

  private val server = new Server()

  def serve(addr: SocketAddress, service: ServiceFactory[Request, Response]) =
    server.serve(addr, service)
}

