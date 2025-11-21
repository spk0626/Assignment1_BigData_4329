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
        // Value Serde: we will handle Order by deserializing using specific Avro deserializer converters (through Serdes)
        // In simple examples, you can use a custom serde or map values to Double for aggregation.

        final StreamsBuilder builder = new StreamsBuilder();

        // read orders as (key=product, value=Order). For simplicity we will deserialize using KafkaAvroDeserializer through usage in application properties
        KStream<String, Order> orders = builder.stream("orders", Consumed.with(Serdes.String(), /* value serde to be provided at runtime by config */ Serdes.serdeFrom(new org.apache.kafka.common.serialization.Serializer<Order>() {
            @Override public byte[] serialize(String topic, Order data) { return null; }
        }, new org.apache.kafka.common.serialization.Deserializer<Order>() {
            @Override public Order deserialize(String topic, byte[] data) { return null; }
        })));

        // NOTE: The above placeholder is to remind: in production you'd implement a proper SpecificAvroSerde wired with Confluent's configs.
        // For simplicity in the classroom assignment, you can instead:
        // - Use the default producer/consumer pattern and run a small separate aggregator that reads orders using KafkaConsumer (typed) and updates a local state (count/sum) and writes averages to a topic.
        //
        // But if you want to use Kafka Streams with Avro, use Confluent's SpecificAvroSerde (from io.confluent.kafka.serializers) and configure it:
        //
        // SpecificAvroSerde<Order> orderSerde = new SpecificAvroSerde<>();
        // Map<String, String> serdeConfig = Map.of(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        // orderSerde.configure(serdeConfig, false);
        //
        // Then:
        // KStream<String, Order> orders = builder.stream("orders", Consumed.with(Serdes.String(), orderSerde));
        //
        // The rest of the aggregation is straightforward:
        //
        // KTable<String, CountAndSum> aggregated = orders
        //   .groupByKey()
        //   .aggregate(() -> new CountAndSum(0, 0.0),
        //      (key, order, agg) -> { agg.count += 1; agg.sum += order.getPrice(); return agg; },
        //      Materialized.with(Serdes.String(), /* serde for CountAndSum - you can use JSON or custom serde */));
        //
        // Then map to average and write to "orders-averages".
        //
        // See Confluent tutorial for a complete ready-to-run Streams average example. :contentReference[oaicite:8]{index=8}
    }
}

