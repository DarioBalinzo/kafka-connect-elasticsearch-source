package com.github.dariobalinzo.filter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class JsonCastFilter implements DocumentFilter {
    private final Set<String> allowedValues;
    private final JsonFilterVisitor visitor;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public JsonCastFilter(Set<String> fieldsToCast) {
        this.allowedValues = fieldsToCast;
        visitor = new JsonFilterVisitor(this::checkIfJsonCastNeeded);
    }

    @Override
    public Map<String, Object> filter(Map<String, Object> document) {
        visitor.visit(document);
        return document;
    }

    private Object checkIfJsonCastNeeded(String key, Object value) {
        if (allowedValues.contains(key)) {
            return castToJson(value);
        } else {
            return value;
        }
    }

    private String castToJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
