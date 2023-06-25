package com.github.dariobalinzo.task;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dariobalinzo.elastic.response.Cursor;


public class CursorSerde {
    final ObjectMapper mapper;


    public CursorSerde() {
        mapper = new ObjectMapper();
    }


    public String serialize(Cursor cursor) throws JsonProcessingException {
        return mapper.writeValueAsString(cursor);
    }


    public Cursor deserialize(String str) throws JsonProcessingException {
        return mapper.readValue(str, Cursor.class);
    }
}
