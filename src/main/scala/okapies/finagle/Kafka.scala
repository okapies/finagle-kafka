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

object KafkaServer {
  import com.twitter.util.{Future, Promise}
  import okapies.finagle.kafka.protocol.{ResponseEncoder, KafkaServerPipelineFactory}
  import com.twitter.finagle.{Stack, ServiceFactory, Service, ListeningServer}
  import com.twitter.finagle.dispatch.GenSerialServerDispatcher
  import com.twitter.finagle.netty3.Netty3Listener
  import com.twitter.finagle.transport.Transport
  import com.twitter.finagle.server.{StdStackServer, StackServer, Listener}
  import org.jboss.netty.buffer.ChannelBuffer
  import org.jboss.netty.util.CharsetUtil
  import org.jboss.netty.channel.{ChannelPipelineFactory, Channels}
  import org.jboss.netty.handler.codec.frame.{LengthFieldPrepender, LengthFieldBasedFrameDecoder}
  import org.jboss.netty.handler.codec.string.{StringDecoder, StringEncoder}
  import java.net.SocketAddress

  object SimpleServerPipeline extends ChannelPipelineFactory {
    def getPipeline = {
      val pipeline = Channels.pipeline()
      // decoders (upstream)
      pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(Int.MaxValue, 0, 4, 0, 4))
      pipeline.addLast("stringDecoder", new StringDecoder(CharsetUtil.UTF_8))
      pipeline.addLast("stringEncoder", new StringEncoder(CharsetUtil.UTF_8))
      pipeline
    }
  }

  class KafkaServerDispatcher(
    trans: Transport[ChannelBuffer, ChannelBuffer],
    service: Service[Request, Response])
  extends GenSerialServerDispatcher[Request, Response, ChannelBuffer, ChannelBuffer](trans) {

    trans.onClose ensure {
      service.close()
    }

    protected def dispatch(msg: ChannelBuffer, eos: Promise[Unit]) = msg match {
      case req: Request =>
        service(req) ensure eos.setDone()
      case failure =>
        eos.setDone
        Future.exception(new IllegalArgumentException(s"Invalid message $failure"))
    }

    protected def handle(resp: Response) = {
      trans.write(ResponseEncoder.encodeResponse(resp))
    }
  }

  case class Server(
    stack: Stack[ServiceFactory[Request, Response]] = StackServer.newStack,
    params: Stack.Params = StackServer.defaultParams
  ) extends StdStackServer[Request, Response, Server] {

    protected type In = ChannelBuffer
    protected type Out = ChannelBuffer

    protected def copy1(
      stack: Stack[ServiceFactory[Request, Response]] = StackServer.newStack,
      params: Stack.Params = StackServer.defaultParams
    ) = copy(stack, params)

    protected def newDispatcher(
      transport: Transport[In, Out],
      service: Service[Request, Response]) = {
      new KafkaServerDispatcher(transport, service)
    }

    protected def newListener(): Listener[In, Out] = {
      Netty3Listener(new KafkaServerPipelineFactory, params)
    }
  }

  val server = Server()

  def serve(
    addr: SocketAddress,
    service: ServiceFactory[Request, Response]): ListeningServer =
    server.serve(addr, service)

}

