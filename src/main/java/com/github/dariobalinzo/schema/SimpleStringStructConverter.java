package com.github.dariobalinzo.schema;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.util.Map;

public class SimpleStringStructConverter implements StructConverter {

    private final ObjectMapper objectMapper = new ObjectMapper();
    @Override
    public Struct convert(Map<String, Object> doc, Schema schema) {
        try {
            return new Struct(schema).put("doc", objectMapper.writeValueAsString(doc));
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
