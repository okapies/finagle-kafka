package okapies.finagle

import com.twitter.util.{Closable, Future, Promise}
import com.twitter.finagle._
import com.twitter.finagle.client.{StackClient, Transporter, StdStackClient}
import com.twitter.finagle.server.{StackServer, StdStackServer, Listener}
import com.twitter.finagle.dispatch.{SerialClientDispatcher, GenSerialServerDispatcher}
import com.twitter.finagle.netty4.{Netty4Transporter, Netty4Listener}
import com.twitter.finagle.pool.SingletonPool
import com.twitter.finagle.transport.{Transport, TransportContext}
import com.twitter.finagle.stats.StatsReceiver
import java.net.SocketAddress

import okapies.finagle.kafka.protocol._

trait KafkaRichClient { self: Client[Request, Response] =>

  def newRichClient(dest: String): kafka.Client = kafka.Client(newService(dest))

  def newRichClient(dest: Name, label: String): kafka.Client = kafka.Client(newService(dest, label))

}

object Kafka
extends Client[Request, Response]
with KafkaRichClient
with Server[Request, Response] {

  case class Client(
    stack: Stack[ServiceFactory[Request, Response]] = StackClient.newStack,
    params: Stack.Params = StackClient.defaultParams
  ) extends StdStackClient[Request, Response, Client] {
    protected type In = Request
    protected type Out = Response
    protected type Context = TransportContext

    protected def copy1(
      stack: Stack[ServiceFactory[Request, Response]],
      params: Stack.Params): Client =
      copy(stack, params)

    protected def newTransporter(addr: SocketAddress): Transporter[Request, Response, TransportContext] =
      Netty4Transporter.raw(KafkaBatchClientPipelineFactory, addr, params)

    protected def newDispatcher(
      transport: Transport[Request, Response] { type Context <: Client.this.Context }
    ): Service[Request, Response] =
      new SerialClientDispatcher(transport)
  }


  val client = Client()

  def newClient(dest: Name, label: String): ServiceFactory[Request, Response] =
    client.newClient(dest, label)

  def newService(dest: Name, label: String): Service[Request, Response] =
    client.newService(dest, label)


  private[finagle] class KafkaServerDispatcher(
    trans: Transport[Response, Request],
    service: Service[Request, Response])
  extends GenSerialServerDispatcher[Request, Response, Response, Request](trans) {

    trans.onClose ensure {
      service.close()
    }

    protected def dispatch(msg: Request, eos: Promise[Unit]): Future[Response] = msg match {
      case req: Request =>
        service(req) ensure eos.setDone()
      case failure =>
        eos.setDone
        Future.exception(new IllegalArgumentException(s"Invalid message $failure"))
    }

    protected def handle(resp: Response): Future[Unit] = resp match {
      case r: NilResponse => Future.Unit // write no response to the transport
      case anyResp => trans.write(resp)
    }
  }


  case class Server(
    stack: Stack[ServiceFactory[Request, Response]] = StackServer.newStack,
    params: Stack.Params = StackServer.defaultParams + param.ProtocolLibrary("kafka")
  ) extends StdStackServer[Request, Response, Server] {
    protected type In = Response
    protected type Out = Request
    protected type Context = TransportContext

    protected def copy1(
      stack: Stack[ServiceFactory[Request, Response]],
      params: Stack.Params): Server =
      copy(stack, params)

    protected def newListener(): Listener[In, Out, TransportContext] =
      Netty4Listener(KafkaServerPipelineFactory, params)

    protected def newDispatcher(
      transport: Transport[In, Out] { type Context <: Server.this.Context },
      service: Service[Request, Response]): Closable =
      new KafkaServerDispatcher(transport, service)
  }

  val server = Server()

  def serve(addr: SocketAddress, service: ServiceFactory[Request, Response]) =
    server.serve(addr, service)
}

