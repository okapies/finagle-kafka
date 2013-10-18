package okapies.finagle.kafka

import com.twitter.finagle.Service

import okapies.finagle.kafka.protocol.{Request, Response}

object Client {

  def apply(raw: Service[Request, Response]): Client = new Client(raw)

}

class Client(service: Service[Request, Response])
