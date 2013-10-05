package okapies.finagle.kafka.protocol;

enum KafkaFrameDecoderState {
    READ_HEADER,
    READ_TOPIC_COUNT,
    READ_TOPIC,
    READ_PARTITION_COUNT,
    READ_PARTITION,
    READ_MESSAGE_SET,
    READ_MESSAGE
}
