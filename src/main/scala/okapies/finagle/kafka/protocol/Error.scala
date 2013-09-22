package okapies.finagle.kafka.protocol

import kafka.common.ErrorMapping

abstract class Error(val code: Short /* int16 */) {

  def throwException() = ErrorMapping.maybeThrowException(code)

}

case object Unknown extends Error(ErrorMapping.UnknownCode)

case object NoError extends Error(ErrorMapping.NoError)

case object OffsetOutOfRange extends Error(ErrorMapping.OffsetOutOfRangeCode)

case object InvalidMessage extends Error(ErrorMapping.InvalidMessageCode)

case object UnknownTopicOrPartition extends Error(ErrorMapping.UnknownTopicOrPartitionCode)

case object InvalidFetchSize extends Error(ErrorMapping.InvalidFetchSizeCode)

case object LeaderNotAvailable extends Error(ErrorMapping.LeaderNotAvailableCode)

case object NotLeaderForPartition extends Error(ErrorMapping.NotLeaderForPartitionCode)

case object RequestTimedOut extends Error(ErrorMapping.RequestTimedOutCode)

case object BrokerNotAvailable extends Error(ErrorMapping.BrokerNotAvailableCode)

case object ReplicaNotAvailable extends Error(ErrorMapping.ReplicaNotAvailableCode)

case object MessageSizeTooLarge extends Error(ErrorMapping.MessageSizeTooLargeCode)

case object StaleControllerEpoch extends Error(ErrorMapping.StaleControllerEpochCode)

//case object OffsetMetadataTooLarge extends KafkaError(ErrorMapping.OffsetMetadataTooLargeCode)

object Error {

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
