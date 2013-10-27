package okapies.finagle.kafka.protocol

/**
 * The number of acknowledgements the server should receive before responding to the request.
 *
 * @param count number of acknowledgements
 */
case class RequiredAcks(count: Short)

object RequiredAcks {

  /**
   * Requires all replicas to commit before responding.
   */
  final val WaitForAllReplicas = RequiredAcks(-1)

  /**
   * Requires no acknowledgement. The client will not receive any response.
   */
  final val WaitForNoAcks = RequiredAcks(0)

  /**
   * Requires only for the leader to commit before responding.
   */
  final val WaitForLeader = RequiredAcks(1)

}
