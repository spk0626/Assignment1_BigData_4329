package com.example.producer;

import com.example.avro.Order;
import io.confluent.kafka.serializers.AbstractKafkaAvroSerDeConfig;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.errors.RetriableException;
import org.apache.kafka.common.errors.SerializationException;

import java.util.Properties;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;

public class ProducerApp {
    public static void main(String[] args) throws Exception {
        String topic = "orders";
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, KafkaAvroSerializer.class.getName());
        props.put(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG, "http://localhost:8081");
        // strong durability and retries
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 3);
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        KafkaProducer<String, Order> producer = new KafkaProducer<>(props);
        // DLQ producer: string serializer so DLQ doesn't depend on schema registry
        Properties dlqProps = new Properties();
        dlqProps.putAll(props);
        dlqProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
        dlqProps.remove(AbstractKafkaAvroSerDeConfig.SCHEMA_REGISTRY_URL_CONFIG);
        KafkaProducer<String, String> dlqProducer = new KafkaProducer<>(dlqProps);

        Random rand = new Random();

        for (int i = 0; i < 50; i++) {
            String orderId = UUID.randomUUID().toString();
            String product = "Item" + (1 + rand.nextInt(5));
            float price = Math.round((1 + rand.nextFloat() * 100) * 100.0f) / 100.0f;

            Order order = Order.newBuilder()
                    .setOrderId(orderId)
                    .setProduct(product)
                    .setPrice(price)
                    .build();

            ProducerRecord<String, Order> rec = new ProducerRecord<>(topic, product, order);
            try {
                sendWithRetry(producer, dlqProducer, rec, "orders-dlq", 3, 1000);
            } catch (Exception e) {
                System.err.println("Failed to send and DLQ also failed: " + e.getMessage());
            }

            Thread.sleep(200); // small gap
        }

        producer.flush();
        producer.close();
        dlqProducer.flush();
        dlqProducer.close();
        System.out.println("Producer finished.");
    }

    private static void sendWithRetry(KafkaProducer<String, Order> producer,
                                      KafkaProducer<String, String> dlqProducer,
                                      ProducerRecord<String, Order> record,
                                      String dlqTopic,
                                      int maxAttempts,
                                      long backoffMs) throws InterruptedException {
        int attempt = 0;
        while (attempt < maxAttempts) {
            attempt++;
            try {
                RecordMetadata md = producer.send(record).get(10, TimeUnit.SECONDS);
                System.out.printf("Sent order=%s key=%s to partition=%d offset=%d%n",
                        ((Order) record.value()).getOrderId(), record.key(), md.partition(), md.offset());
                return;
            } catch (ExecutionException ex) {
                Throwable cause = ex.getCause();
                if (cause instanceof RetriableException) {
                    System.err.println("Retriable send error, attempt " + attempt + ": " + cause.getMessage());
                } else if (cause instanceof java.net.ConnectException) {
                    System.err.println("Network error, attempt " + attempt + ": " + cause.getMessage());
                } else if (cause instanceof SerializationException) {
                    System.err.println("Serialization error (non-retriable): " + cause.getMessage());
                    // send to DLQ as JSON string
                    sendToDlq(dlqProducer, dlqTopic, record);
                    return;
                } else {
                    System.err.println("Non-retriable send error: " + cause.getMessage());
                    // final attempt: send to DLQ
                    sendToDlq(dlqProducer, dlqTopic, record);
                    return;
                }
            } catch (Exception ex) {
                // Timeout or interruption
                System.err.println("Send exception (attempt " + attempt + "): " + ex.getMessage());
            }

            if (attempt < maxAttempts) {
                Thread.sleep(backoffMs);
            }
        }

        // exhausted retries -> write to DLQ
        sendToDlq(dlqProducer, dlqTopic, record);
    }

    private static void sendToDlq(KafkaProducer<String, String> dlqProducer,
                                  String dlqTopic,
                                  ProducerRecord<String, Order> original) {
        try {
            String json = original.value().toString();
            ProducerRecord<String, String> dlqRec = new ProducerRecord<>(dlqTopic, original.key(), json);
            dlqProducer.send(dlqRec).get(5, TimeUnit.SECONDS);
            System.err.println("Sent message to DLQ topic=" + dlqTopic + " key=" + original.key());
        } catch (Exception e) {
            System.err.println("Failed to write to DLQ: " + e.getMessage());
        }
    }

}

