package com.github.dariobalinzo.task;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dariobalinzo.elastic.response.Cursor;

import java.util.Objects;


public class CursorSerde {
    final ObjectMapper mapper;


    public CursorSerde() {
        mapper = new ObjectMapper();
    }


    public String serialize(Cursor cursor) {
        if (cursor == null) {
            return null;
        }
        try {
            return mapper.writeValueAsString(cursor);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }


    public Cursor deserialize(String str) {
        if (str == null) {
            return null;
        }
        try {
            return mapper.readValue(str, Cursor.class);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
