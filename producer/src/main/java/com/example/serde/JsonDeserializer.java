package com.example.serde;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.common.serialization.Deserializer;

import java.util.Map;

public class JsonDeserializer<T> implements Deserializer<T> {
    private final ObjectMapper mapper = new ObjectMapper();
    private final Class<T> clazz;

    public JsonDeserializer(Class<T> clazz) { this.clazz = clazz; }

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) { }

    @Override
    public T deserialize(String topic, byte[] data) {
        try {
            if (data == null) return null;
            return mapper.readValue(data, clazz);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() { }
}
