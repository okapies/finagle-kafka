finagle-kafka
=============

An Apache Kafka client in Netty and Finagle.

## Overview
*finagle-kafka* is an [Apache Kafka](https://kafka.apache.org/) client in [Netty](http://netty.io/)
and [Twitter's Finagle](http://twitter.github.io/finagle/). It enables you to handle Kafka in more
functional and composable way based on [Futures](http://twitter.github.io/finagle/guide/Futures.html).

You can also utilize excellent features of Finagle: *load balancing, connection pooling, timeouts,
retries and bunch of statistics for monitoring and diagnostics.*

**Your feedbacks and contributions are welcome!**

## Setup
```
libraryDependencies += "com.github.okapies" %% "finagle-kafka" % "0.2.1"
```

## Usage
```
import com.twitter.util.Future
import okapies.finagle._
import okapies.finagle.kafka.protocol._

// connect to bootstrap broker
val bootstrap = Kafka.newRichClient("[host]:[port]")

// create a client for the leader of specific topic partition
val metadata = bootstrap.metadata("topic")
val client = metadata.map {
  _.head.partitions(0).leader.map { l =>
    Kafka.newRichClient(s"${l.host}:${l.port}")
  }.get
}

client.foreach { cli =>
  // get offset
  val offset = cli.offset("topic", 0, -1).map(_.offsets(0))

  // produce a message
  val produced = offset.unit before cli.produce("topic", 0, "foobar").unit

  // fetch messages
  val msgs = produced before offset.flatMap { offset =>
    cli.fetch("topic", 0, offset).map {
      _.messages.map(_.message.value.toString("UTF-8"))
    }
  }
  msgs.foreach(_.foreach(println))
}
```

## Build
```
$ git clone https://github.com/okapies/finagle-kafka.git
$ cd finagle-kafka
$ sbt package
```

## Running tests

Tests are run using sbt.

```
sbt test
```

## Future work
- Compression support
- Partitioning and Zookeeper support
- Migration to Netty 4/5 and Finagle 7
- More high-level and sophisticated `KafkaClient`
- Compatibility test
