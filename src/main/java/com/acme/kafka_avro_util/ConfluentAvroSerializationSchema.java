package com.acme.kafka_avro_util;

import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.flink.streaming.util.serialization.SerializationSchema;

public class ConfluentAvroSerializationSchema<T> implements SerializationSchema<T> {
    private static final long serialVersionUID = 1L;
    private final String topic;
    private final KafkaAvroSerializer avroSerializer;

    public ConfluentAvroSerializationSchema(String topic, KafkaAvroSerializer avroSerializer) {
        this.topic =topic;
        this.avroSerializer = avroSerializer;
    }

    @Override
    public byte[] serialize(T obj) {
        return avroSerializer.serialize(topic, obj);
    }
}
