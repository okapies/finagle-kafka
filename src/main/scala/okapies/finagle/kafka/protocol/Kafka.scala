package okapies.finagle.kafka.protocol

import java.util.concurrent.ConcurrentHashMap

import org.jboss.netty.channel.{ChannelPipelineFactory, Channels}
import org.jboss.netty.handler.codec.frame.{LengthFieldPrepender, LengthFieldBasedFrameDecoder}

import com.twitter.finagle._
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}

object Kafka {
  def apply() = new Kafka()
  def apply(stats: StatsReceiver = NullStatsReceiver) = new Kafka(stats)
  def get() = apply()
}

class KafkaServerPipelineFactory extends ChannelPipelineFactory {
  def getPipeline() = {
    val pipeline = Channels.pipeline()

    pipeline
  }
}

private[protocol] trait RequestLogger {

  def append(req: Request): Unit

}

private[protocol] class CorrelationSelector extends (Int => Option[Short]) with RequestLogger {

  private[this] val requests = new ConcurrentHashMap[Int, Short]

  def apply(correlationId: Int) = Option(requests.remove(correlationId))

  def append(req: Request) = {
    requests.putIfAbsent(req.correlationId, Request.toApiKey(req))
    // TODO: handle duplicate correlationId
  }

}

object KafkaClientPipelineFactory extends ChannelPipelineFactory {
  def getPipeline() = {
    val pipeline = Channels.pipeline()

    val selector = new CorrelationSelector

    // encoders (downstream)
    pipeline.addLast("frameEncoder", new LengthFieldPrepender(4))
    pipeline.addLast("requestEncoder", new RequestEncoder(selector))

    // decoders (upstream)
    pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(8192, 0, 4, 0, 4))
    pipeline.addLast("responseDecoder", new ResponseDecoder(selector))

    pipeline
  }
}

class Kafka(stats: StatsReceiver) extends CodecFactory[Request, Response] {

  def this() = this(NullStatsReceiver)

  def server: ServerCodecConfig => Codec[Request, Response] =
    Function.const {
      new Codec[Request, Response] {
        def pipelineFactory = new KafkaServerPipelineFactory
      }
    }

  def client: ClientCodecConfig => Codec[Request, Response] =
    Function.const {
      new Codec[Request, Response] {
        def pipelineFactory = KafkaClientPipelineFactory

        override def prepareConnFactory(underlying: ServiceFactory[Request, Response]) = {
          new KafkaTracingFilter() andThen new KafkaLoggingFilter(stats) andThen underlying
        }
      }
    }

}

private class KafkaTracingFilter extends SimpleFilter[Request, Response] {

  override def apply(request: Request, service: Service[Request, Response]) = service(request)

}

private class KafkaLoggingFilter(stats: StatsReceiver)
  extends SimpleFilter[Request, Response] {

  private[this] val error = stats.scope("error")
  private[this] val succ  = stats.scope("success")

  override def apply(request: Request, service: Service[Request, Response]) = service(request)

}
