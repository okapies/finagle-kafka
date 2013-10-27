package okapies.finagle.kafka.protocol

/**
 * A partition within a topic.
 */
case class TopicPartition(topic: String /* string */, partition: Int /* int32 */) {

  override def toString = "[%s,%d]".format(topic, partition)

}
