package okapies.finagle.kafka.protocol

import _root_.kafka.common.ErrorMapping

case class KafkaError(code: Short /* int16 */) {

  def throwException() = ErrorMapping.maybeThrowException(code)

  override def toString = code match {
    case ErrorMapping.UnknownCode => "Unknown"
    case ErrorMapping.NoError => "NoError"
    case ErrorMapping.OffsetOutOfRangeCode => "OffsetOutOfRange"
    case ErrorMapping.InvalidMessageCode => "InvalidMessage"
    case ErrorMapping.UnknownTopicOrPartitionCode => "UnknownTopicOrPartition"
    case ErrorMapping.InvalidFetchSizeCode => "InvalidFetchSize"
    case ErrorMapping.LeaderNotAvailableCode => "LeaderNotAvailable"
    case ErrorMapping.NotLeaderForPartitionCode => "NotLeaderForPartition"
    case ErrorMapping.RequestTimedOutCode => "RequestTimedOut"
    case ErrorMapping.BrokerNotAvailableCode => "BrokerNotAvailable"
    case ErrorMapping.ReplicaNotAvailableCode => "ReplicaNotAvailable"
    case ErrorMapping.MessageSizeTooLargeCode => "MessageSizeTooLarge"
    case ErrorMapping.StaleControllerEpochCode => "StaleControllerEpoch"
    case ErrorMapping.OffsetMetadataTooLargeCode => "OffsetMetadataTooLarge"
    case ErrorMapping.OffsetsLoadInProgressCode => "OffsetsLoadInProgress"
    case ErrorMapping.ConsumerCoordinatorNotAvailableCode => "ConsumerCoordinatorNotAvailable"
    case ErrorMapping.NotCoordinatorForConsumerCode => "NotCoordinatorForConsumer"
    case _ => super.toString
  }

}

object KafkaError {

  final val Unknown = KafkaError(ErrorMapping.UnknownCode)

  final val NoError = KafkaError(ErrorMapping.NoError)

  final val OffsetOutOfRange = KafkaError(ErrorMapping.OffsetOutOfRangeCode)

  final val InvalidMessage = KafkaError(ErrorMapping.InvalidMessageCode)

  final val UnknownTopicOrPartition = KafkaError(ErrorMapping.UnknownTopicOrPartitionCode)

  final val InvalidFetchSize = KafkaError(ErrorMapping.InvalidFetchSizeCode)

  final val LeaderNotAvailable = KafkaError(ErrorMapping.LeaderNotAvailableCode)

  final val NotLeaderForPartition = KafkaError(ErrorMapping.NotLeaderForPartitionCode)

  final val RequestTimedOut = KafkaError(ErrorMapping.RequestTimedOutCode)

  final val BrokerNotAvailable = KafkaError(ErrorMapping.BrokerNotAvailableCode)

  final val ReplicaNotAvailable = KafkaError(ErrorMapping.ReplicaNotAvailableCode)

  final val MessageSizeTooLarge = KafkaError(ErrorMapping.MessageSizeTooLargeCode)

  final val StaleControllerEpoch = KafkaError(ErrorMapping.StaleControllerEpochCode)

  final val OffsetMetadataTooLarge = KafkaError(ErrorMapping.OffsetMetadataTooLargeCode)

  final val OffsetsLoadInProgress = KafkaError(ErrorMapping.OffsetsLoadInProgressCode) 

  final val ConsumerCoordinatorNotAvailable = KafkaError(ErrorMapping.ConsumerCoordinatorNotAvailableCode)

  final val NotCoordinatorForConsumer = KafkaError(ErrorMapping.NotCoordinatorForConsumerCode)
}

class KafkaCodecException(message: String = null, cause: Throwable = null)
  extends RuntimeException(message: String, cause: Throwable)
