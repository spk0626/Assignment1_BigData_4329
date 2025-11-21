package com.example.consumer;

import com.example.avro.Order;

import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import io.confluent.kafka.serializers.KafkaAvroSerializer;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.producer.ProducerConfig;

import java.io.IOException;
import java.time.Duration;
import java.util.*;

public class ConsumerApp {

    private static final String TOPIC = "orders";
    private static final String DLQ_TOPIC = "orders-dlq";
    private static final int MAX_RETRIES = 3;

    public static void main(String[] args) {

        // Consumer COnfig
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "order-consumer-group");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, KafkaAvroDeserializer.class.getName());

        consumerProps.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        consumerProps.put("specific.avro.reader", true);

        KafkaConsumer<String, Order> consumer = new KafkaConsumer<>(consumerProps);
        consumer.subscribe(Collections.singletonList(TOPIC));


        // DLQ producer config
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());

        producerProps.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");

        KafkaProducer<String, Order> dlqProducer = new KafkaProducer<>(producerProps);


        // Main consumption loop
        try {
            while (true) {

                ConsumerRecords<String, Order> records =
                        consumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, Order> rec : records) {

                    boolean processed = processWithRetry(rec, dlqProducer);


                    TopicPartition tp = new TopicPartition(rec.topic(), rec.partition());
                    OffsetAndMetadata om = new OffsetAndMetadata(rec.offset() + 1);

                    consumer.commitSync(Collections.singletonMap(tp, om));
                }
            }

        } finally {
            consumer.close();
            dlqProducer.close();
        }
    }


    // Retry logic for processing
    private static boolean processWithRetry(
            ConsumerRecord<String, Order> rec,
            KafkaProducer<String, Order> dlqProducer) {

        int attempt = 0;

        while (attempt <= MAX_RETRIES) {
            attempt++;

            try {
                processOrder(rec.value());
                System.out.println("Processed order: " + rec.value().getOrderId());
                return true;

            } catch (TransientProcessingException te) {
                System.err.println("Retryable error for order " + rec.value().getOrderId() +
                        " attempt " + attempt + "/" + MAX_RETRIES);

                try {
                    Thread.sleep((long) Math.pow(2, attempt) * 500);
                } catch (InterruptedException ignored) {
                }

                if (attempt > MAX_RETRIES) {
                    sendToDLQ(rec, dlqProducer);
                    return false;
                }

            } catch (Exception e) {
                System.err.println("Non-retryable error: " + e.getMessage());
                sendToDLQ(rec, dlqProducer);
                return false;
            }
        }
        return false;
    }


    // DLQ logic
    private static void sendToDLQ(ConsumerRecord<String, Order> rec,
                                  KafkaProducer<String, Order> dlqProducer) {

        ProducerRecord<String, Order> dlqRecord =
                new ProducerRecord<>(DLQ_TOPIC, rec.key(), rec.value());

        dlqProducer.send(dlqRecord, (meta, ex) -> {
            if (ex != null) {
                System.err.println("Failed to publish to DLQ: " + ex.getMessage());
            } else {
                System.err.println("Sent to DLQ partition=" + meta.partition() +
                        " offset=" + meta.offset());
            }
        });

        dlqProducer.flush();
    }


    // Order processing 
    private static void processOrder(Order order)
            throws TransientProcessingException {

        double r = Math.random();

        if (r < 0.1) {
            throw new RuntimeException("Non-retryable failure for order " + order.getOrderId());
        } else if (r < 0.3) {
            throw new TransientProcessingException(
                    "Transient simulated failure for order " + order.getOrderId());
        }
    }

    static class TransientProcessingException extends IOException {
        public TransientProcessingException(String msg) {
            super(msg);
        }
    }
}
