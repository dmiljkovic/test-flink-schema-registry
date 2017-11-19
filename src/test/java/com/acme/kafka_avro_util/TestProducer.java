package com.acme.kafka_avro_util;

import com.acme.kafka_avro_util.TestRecordAvro;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.*;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.util.StreamingMultipleProgramsTestBase;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.streams.integration.utils.EmbeddedKafkaCluster;
import org.junit.Test;

import java.util.*;

public class TestProducer extends StreamingMultipleProgramsTestBase {
    @org.junit.Test
    public void testAvroConsumer() throws Exception {
        // kafka cluster init
        EmbeddedKafkaCluster kafkaCluster = new EmbeddedKafkaCluster(1);
        kafkaCluster.start();
        String topic = "avro_test";
        kafkaCluster.deleteAndRecreateTopics(topic);

        //schema registry
        Properties registryProp = new Properties();
        registryProp.put(KafkaAvroDeserializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "bogus");
        MockSchemaRegistryClient schemaRegistry = new MockSchemaRegistryClient();
        schemaRegistry.register(topic, TestRecordAvro.getClassSchema());

        //KafkaAvroEncoder avroEncoder = new KafkaAvroEncoder(schemaRegistry);
        //KafkaAvroDeserializer avroDeserializer = new KafkaAvroDeserializer(schemaRegistry);
        //KafkaAvroDecoder avroDecoder = new KafkaAvroDecoder(schemaRegistry, new VerifiableProperties(defaultConfig));

        //get flink env
        Properties flinkProp = new Properties();
        flinkProp.setProperty("bootstrap.servers", kafkaCluster.bootstrapServers());
        flinkProp.setProperty("zookeeper.connect", kafkaCluster.zKConnectString());
        flinkProp.setProperty("group.id", "test");
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // cteate data stream
        List<TestRecordAvro> rec = new ArrayList<>();
        TestRecordAvro r1 = new TestRecordAvro();
        r1.setId("a1");
        TestRecordAvro r2 = new TestRecordAvro();
        r2.setId("a2");
        rec.add(r1);
        rec.add(r2);
        DataStream<TestRecordAvro> ds = env.fromCollection(rec);

        //serialize avro
        KafkaAvroSerializer avroSerializer = new KafkaAvroSerializer(schemaRegistry);
        avroSerializer.configure(new HashMap(registryProp), false);
        ConfluentAvroSerializationSchema ser =
                new ConfluentAvroSerializationSchema<TestRecordAvro>(topic, avroSerializer);

        //write to kafka
        FlinkKafkaProducer010.writeToKafkaWithTimestamps(ds, topic, ser, flinkProp);

        // execute flink
        env.execute();
    }
}
