package okapies.finagle.kafka.protocol

import kafka.common.ErrorMapping

abstract class KafkaError(val code: Short /* int16 */) {

  def throwException() = ErrorMapping.maybeThrowException(code)

}

case object Unknown extends KafkaError(ErrorMapping.UnknownCode)

case object NoError extends KafkaError(ErrorMapping.NoError)

case object OffsetOutOfRange extends KafkaError(ErrorMapping.OffsetOutOfRangeCode)

case object InvalidMessage extends KafkaError(ErrorMapping.InvalidMessageCode)

case object UnknownTopicOrPartition extends KafkaError(ErrorMapping.UnknownTopicOrPartitionCode)

case object InvalidFetchSize extends KafkaError(ErrorMapping.InvalidFetchSizeCode)

case object LeaderNotAvailable extends KafkaError(ErrorMapping.LeaderNotAvailableCode)

case object NotLeaderForPartition extends KafkaError(ErrorMapping.NotLeaderForPartitionCode)

case object RequestTimedOut extends KafkaError(ErrorMapping.RequestTimedOutCode)

case object BrokerNotAvailable extends KafkaError(ErrorMapping.BrokerNotAvailableCode)

case object ReplicaNotAvailable extends KafkaError(ErrorMapping.ReplicaNotAvailableCode)

case object MessageSizeTooLarge extends KafkaError(ErrorMapping.MessageSizeTooLargeCode)

case object StaleControllerEpoch extends KafkaError(ErrorMapping.StaleControllerEpochCode)

//case object OffsetMetadataTooLarge extends KafkaError(ErrorMapping.OffsetMetadataTooLargeCode)

object KafkaError {

  def apply(code: Short) = toError(code)

  private[this] val toError = Map(
    ErrorMapping.UnknownCode -> Unknown,
    ErrorMapping.NoError -> NoError,
    ErrorMapping.OffsetOutOfRangeCode -> OffsetOutOfRange,
    ErrorMapping.InvalidMessageCode -> InvalidMessage,
    ErrorMapping.UnknownTopicOrPartitionCode -> UnknownTopicOrPartition,
    ErrorMapping.InvalidFetchSizeCode -> InvalidFetchSize,
    ErrorMapping.LeaderNotAvailableCode -> LeaderNotAvailable,
    ErrorMapping.NotLeaderForPartitionCode -> NotLeaderForPartition,
    ErrorMapping.RequestTimedOutCode -> RequestTimedOut,
    ErrorMapping.BrokerNotAvailableCode -> BrokerNotAvailable,
    ErrorMapping.ReplicaNotAvailableCode -> ReplicaNotAvailable,
    ErrorMapping.MessageSizeTooLargeCode -> MessageSizeTooLarge,
    ErrorMapping.StaleControllerEpochCode -> StaleControllerEpoch
    //ErrorMapping.OffsetMetadataTooLargeCode -> OffsetMetadataTooLarge
  )

}
