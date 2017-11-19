package com.acme.kafka_avro_util;


import io.confluent.kafka.schemaregistry.client.CachedSchemaRegistryClient;
import io.confluent.kafka.schemaregistry.client.SchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroDecoder;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.util.serialization.DeserializationSchema;


public class ConfluentAvroDeserializationSchema implements DeserializationSchema<String> {

    private final String schemaRegistryUrl;
    private final int identityMapCapacity;
    private KafkaAvroDecoder kafkaAvroDecoder;

    public ConfluentAvroDeserializationSchema(String schemaRegistyUrl) {
        this(schemaRegistyUrl, 1000);
    }

    public ConfluentAvroDeserializationSchema(String schemaRegistryUrl, int identityMapCapacity) {
        this.schemaRegistryUrl = schemaRegistryUrl;
        this.identityMapCapacity = identityMapCapacity;
    }

    @Override
    public String deserialize(byte[] message) {
        if (kafkaAvroDecoder == null) {
            SchemaRegistryClient schemaRegistry = new CachedSchemaRegistryClient(this.schemaRegistryUrl, this.identityMapCapacity);
            this.kafkaAvroDecoder = new KafkaAvroDecoder(schemaRegistry);
        }
        return this.kafkaAvroDecoder.fromBytes(message).toString();
    }

    @Override
    public boolean isEndOfStream(String nextElement) {
        return false;
    }

    @Override
    public TypeInformation<String> getProducedType() {
        return BasicTypeInfo.STRING_TYPE_INFO;
    }

}
