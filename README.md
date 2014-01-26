finagle-kafka
=============

An Apache Kafka client in Netty and Finagle.

## Usage

```
import com.twitter.util.Future
import okapies.finagle._
import okapies.finagle.kafka.protocol._

// connect to bootstrap broker
val bootstrap = Kafka.newRichClient("[host]:[port]")

// create a client for the leader of specific topic partition
val client = for {
  metadata <- bootstrap.metadata("topic")
  leader <- Future.value(metadata.head.partitions(0).leader.get)
  client <- Future.value(Kafka.newRichClient(s"${leader.host}:${leader.port}"))
} yield client

// get offset
val offset = client.map(_.offset("topic", 0, -1).get.offsets(0)).get

// produce a message
client.foreach(_.produce("topic", 0, "foobar"))

// fetch messages
val msgs = client.flatMap {
  _.fetch("topic", 0, offset).map {
    _.messages.map(_.message.value.toString("UTF-8"))
  }
}
msgs.foreach(_.foreach(println))
```
