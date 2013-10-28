package okapies.finagle.kafka.protocol

import scala.collection._

import org.jboss.netty.channel.{ChannelPipelineFactory, Channels}
import org.jboss.netty.handler.codec.frame.{LengthFieldPrepender, LengthFieldBasedFrameDecoder}

import com.twitter.finagle._
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}

object KafkaCodec {

  def apply() = new KafkaCodecFactory()

  def apply(stats: StatsReceiver = NullStatsReceiver) = new KafkaCodecFactory(stats)

  def get() = apply()

}

trait RequestCorrelator extends (Int => Option[Short])

object RequestCorrelator {

  def apply(f: Int => Option[Short]) = new RequestCorrelator {
    def apply(correlationId: Int): Option[Short] = f(correlationId)
  }

}

trait RequestLogger {

  def add(req: Request): Unit

  def remove(req: Request): Unit

}

class QueueRequestCorrelator extends RequestCorrelator with RequestLogger {

  private[this] val queue = new mutable.Queue[Short]

  def apply(correlationId: Int) = queue.isEmpty match {
    case false => Some(queue.dequeue())
    case true => None
  }

  def add(req: Request) = queue += Request.toApiKey(req)

  def remove(req: Request) = queue.isEmpty match {
    case false => queue.dequeue()
    case true =>
  }

}

class CorrelationIdRequestCorrelator extends RequestCorrelator with RequestLogger {

  private[this] val requests = new mutable.HashMap[Int, Short]

  def apply(correlationId: Int) = requests.remove(correlationId)

  def add(req: Request) {
    requests.put(req.correlationId, Request.toApiKey(req))
    // TODO: handle duplicate correlationId
  }

  def remove(req: Request) = requests.remove(req.correlationId)

}

class KafkaServerPipelineFactory extends ChannelPipelineFactory {
  def getPipeline() = {
    val pipeline = Channels.pipeline()

    pipeline
  }
}

object KafkaBatchClientPipelineFactory extends ChannelPipelineFactory {

  def getPipeline() = {
    val pipeline = Channels.pipeline()

    val correlator = new QueueRequestCorrelator

    // encoders (downstream)
    pipeline.addLast("frameEncoder", new LengthFieldPrepender(4))
    pipeline.addLast("requestEncoder", new RequestEncoder(correlator))

    // decoders (upstream)
    pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(8192, 0, 4, 0, 4))
    pipeline.addLast("responseDecoder", new BatchResponseDecoder(correlator))

    pipeline
  }

}

object KafkaStreamClientPipelineFactory extends ChannelPipelineFactory {

  def getPipeline() = {
    val pipeline = Channels.pipeline()

    val correlator = new QueueRequestCorrelator

    // encoders (downstream)
    pipeline.addLast("frameEncoder", new LengthFieldPrepender(4))
    pipeline.addLast("requestEncoder", new RequestEncoder(correlator))

    // decoders (upstream)
    pipeline.addLast("frameDecoder", new StreamFrameDecoder(correlator, 8192))
    pipeline.addLast("responseDecoder", new StreamResponseDecoder)

    pipeline
  }

}

class KafkaCodecFactory(stats: StatsReceiver) extends CodecFactory[Request, Response] {

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
        def pipelineFactory = KafkaStreamClientPipelineFactory

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
