package com.example.streams;

import com.example.avro.Order;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.*;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;

import java.util.Properties;

public class StreamsAverage {

    public static class CountAndSum {
        public long count;
        public double sum;
        public CountAndSum() {}
        public CountAndSum(long count, double sum) { this.count = count; this.sum = sum; }
    }

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "orders-average-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());


        final StreamsBuilder builder = new StreamsBuilder();


        KStream<String, Order> orders = builder.stream("orders", Consumed.with(Serdes.String(),  Serdes.serdeFrom(new org.apache.kafka.common.serialization.Serializer<Order>() {
            @Override public byte[] serialize(String topic, Order data) { return null; }
        }, new org.apache.kafka.common.serialization.Deserializer<Order>() {
            @Override public Order deserialize(String topic, byte[] data) { return null; }
        })));

    }
}

