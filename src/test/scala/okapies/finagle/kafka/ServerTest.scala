package okapies.finagle.kafka

import org.scalatest._
import org.scalatest.matchers._
import org.jboss.netty.buffer.ChannelBuffer
import com.twitter.concurrent.AsyncQueue
import com.twitter.util.Future
import com.twitter.finagle.Service
import com.twitter.finagle.transport.{Transport, QueueTransport}

import okapies.finagle.KafkaServer
import protocol.{Request, Response}

class ServerTest
extends FlatSpec
with Matchers {
  val clientToServer = new AsyncQueue[ChannelBuffer]
  val serverToClient = new AsyncQueue[ChannelBuffer]
  val transport = new QueueTransport(writeq = serverToClient, readq = clientToServer)
  val service = Service.mk[Request, Response] { req => Future.value(null) }

  val dispatch = new KafkaServer.KafkaServerDispatcher(transport, service)

  // tests the server dispatcher
  "A Kafka Server" should "start and shutdown" in {
    1 should be(1)
  }
}

