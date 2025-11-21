package com.example.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Serializer;

import java.util.Map;

public class JsonSerializer<T> implements Serializer<T> {
    private final ObjectMapper mapper = new ObjectMapper();

    public JsonSerializer() {}

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) { }

    @Override
    public byte[] serialize(String topic, T data) {
        try {
            if (data == null) return null;
            return mapper.writeValueAsBytes(data);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() { }
}
