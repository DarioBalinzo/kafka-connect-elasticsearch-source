package com.github.dariobalinzo.task;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dariobalinzo.elastic.response.Cursor;

import java.util.Map;

public class OffsetSerializer {

    private final CursorSerde cursorSerde;

    public OffsetSerializer() {
        this(new CursorSerde(new ObjectMapper()));
    }

    public OffsetSerializer(CursorSerde cursorSerde) {
        this.cursorSerde = cursorSerde;
    }


    public Map<String, Object> serialize(Cursor cursor) {
        return Map.of("position", cursorSerde.serialize(cursor));
    }

    public Cursor deserialize(Map<String, Object> offset) {
        if (offset == null) {
            return null;
        }
        return cursorSerde.deserialize(String.valueOf(offset.get("position")));
    }

    public static class CursorSerde {

        private final ObjectMapper objectMapper;

        public CursorSerde(ObjectMapper objectMapper) {
            this.objectMapper = objectMapper;
        }


        public String serialize(Cursor cursor) {
            if (cursor == null) {
                return null;
            }
            try {
                return objectMapper.writeValueAsString(cursor);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }


        public Cursor deserialize(String str) {
            if (str == null) {
                return null;
            }
            try {
                return objectMapper.readValue(str, Cursor.class);
            } catch (JsonProcessingException e) {
                throw new RuntimeException(e);
            }
        }
    }
}
