package com.example.streams;

import com.example.avro.Order;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;

import com.example.serde.JsonDeserializer;
import com.example.serde.JsonSerializer;
import com.example.streams.StreamsApp.Aggregate;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Streams app computing running average price per product and writing JSON to `orders-averages`.
 */
public class StreamsApp {
    public static class Aggregate {
        public double sum;
        public long count;

        public Aggregate() {}
        public Aggregate(double sum, long count) { this.sum = sum; this.count = count; }
        public double average() { return count == 0 ? 0.0 : sum / count; }
    }

    public static void main(String[] args) {
        String bootstrap = "localhost:9092";
        String schemaRegistry = "http://localhost:8081";

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "orders-averages-app");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrap);
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry);

        StreamsBuilder builder = new StreamsBuilder();

        // use KafkaAvro (de)serializers for Order
        Map<String, String> serdeConfig = new HashMap<>();
        serdeConfig.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, schemaRegistry);

        @SuppressWarnings({"rawtypes", "unchecked"})
        Serializer<Order> orderSerializer = (Serializer) new KafkaAvroSerializer();
        @SuppressWarnings({"rawtypes", "unchecked"})
        Deserializer<Order> orderDeserializer = (Deserializer) new KafkaAvroDeserializer();
        Serde<Order> orderSerde = Serdes.serdeFrom(orderSerializer, orderDeserializer);

        KStream<String, Order> orders = builder.stream("orders", Consumed.with(Serdes.String(), orderSerde));

        // Group by product (use product as key)
        KStream<String, Order> keyedByProduct = orders.selectKey((k, v) -> v.getProduct() == null ? null : v.getProduct().toString());
        KGroupedStream<String, Order> byProduct = keyedByProduct.groupByKey(Grouped.with(Serdes.String(), orderSerde));

        // JSON serde for Aggregate
        JsonSerializer<Aggregate> aggSer = new JsonSerializer<>();
        JsonDeserializer<Aggregate> aggDes = new JsonDeserializer<>(Aggregate.class);
        Serde<Aggregate> aggSerde = Serdes.serdeFrom(aggSer, aggDes);

        KTable<String, Aggregate> aggregates = byProduct.aggregate(
                () -> new Aggregate(0.0, 0L),
                (key, order, agg) -> {
                    agg.sum += order.getPrice();
                    agg.count += 1;
                    return agg;
                },
                Materialized.with(Serdes.String(), aggSerde)
        );

        aggregates.toStream().mapValues(agg -> String.format("{\"average\":%.2f,\"count\":%d}", agg.average(), agg.count))
                .to("orders-averages", Produced.with(Serdes.String(), Serdes.String()));

        KafkaStreams streams = new KafkaStreams(builder.build(), props);
        streams.start();
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }
}
