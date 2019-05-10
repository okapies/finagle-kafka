package okapies.finagle.kafka.protocol

import scala.collection._

import io.netty.channel.ChannelPipeline
import io.netty.handler.codec.{LengthFieldPrepender, LengthFieldBasedFrameDecoder}

import com.twitter.finagle._
import com.twitter.finagle.stats.{NullStatsReceiver, StatsReceiver}

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

object KafkaServerPipelineFactory extends (ChannelPipeline => Unit) {

  def apply(pipeline: ChannelPipeline) = {
    // decoders (upstream)
    
    /**
     * Kafka request protocol
     * {{{
     * RequestOrResponse => Size (RequestMessage | ResponseMessage)
     *    Size => int32
     * }}}
     */
    pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(Int.MaxValue, 0, 4, 0, 4))
    // actual request to server
    pipeline.addLast("requestDecoder", new RequestDecoder())

    // encoders (downstream)
    pipeline.addLast("frameEncoder", new LengthFieldPrepender(4))
    pipeline.addLast("responseEncoder", new ResponseEncoder())
  }
}

object KafkaBatchClientPipelineFactory extends (ChannelPipeline => Unit) {

  def apply(pipeline: ChannelPipeline) = {
    val correlator = new QueueRequestCorrelator

    // encoders (downstream)
    pipeline.addLast("frameEncoder", new LengthFieldPrepender(4))
    pipeline.addLast("requestEncoder", new RequestEncoder(correlator))

    // decoders (upstream)
    pipeline.addLast("frameDecoder", new LengthFieldBasedFrameDecoder(Int.MaxValue, 0, 4, 0, 4))
    pipeline.addLast("responseDecoder", new BatchResponseDecoder(correlator))
  }

}

object KafkaStreamClientPipelineFactory extends (ChannelPipeline => Unit) {

  def apply(pipeline: ChannelPipeline) = {
    val correlator = new QueueRequestCorrelator

    // encoders (downstream)
    pipeline.addLast("frameEncoder", new LengthFieldPrepender(4))
    pipeline.addLast("requestEncoder", new RequestEncoder(correlator))

    // decoders (upstream)
    pipeline.addLast("frameDecoder", new StreamFrameDecoder(correlator, 8192))
    pipeline.addLast("responseDecoder", new StreamResponseDecoder)
  }

}

