package com.github.dariobalinzo.task;

import co.elastic.clients.elasticsearch._types.FieldValue;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.*;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.github.dariobalinzo.Pair;
import com.github.dariobalinzo.elastic.response.Cursor;

import java.io.IOException;
import java.util.Map;
import java.util.Objects;

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


            var fieldValueSerializer = new JsonSerializer<FieldValue>() {
                @Override
                public void serialize(FieldValue value, JsonGenerator gen, SerializerProvider serializers) throws IOException {
                    var lhs = value._kind();

                    if (Objects.requireNonNull(value._kind()) == FieldValue.Kind.String) {
                        gen.writeObject(new Pair<>(lhs, value.stringValue()));
                    } else if (value._kind() == FieldValue.Kind.Long) {
                        gen.writeObject(new Pair<>(lhs, value.longValue()));
                    } else if (value._kind() == FieldValue.Kind.Double) {
                        gen.writeObject(new Pair<>(lhs, value.doubleValue()));
                    } else if (value._kind() == FieldValue.Kind.Boolean) {
                        gen.writeObject(new Pair<>(lhs, value.booleanValue()));
                    }
                }
            };

            var fieldValueDeserializer = new JsonDeserializer<FieldValue>() {
                @Override
                public FieldValue deserialize(JsonParser p, DeserializationContext ctxt) throws IOException, JsonProcessingException {
                    var pair = p.readValueAs(Pair.class);
                    if (pair.getL().equals("String")) {
                        return new FieldValue.Builder().stringValue((String) pair.getR()).build();
                    } else if (pair.getL().equals("Long")) {
                        return new FieldValue.Builder().longValue(Long.parseLong(pair.getR().toString())).build();
                    } else if (pair.getL().equals("Double")) {
                        return new FieldValue.Builder().doubleValue((Double) pair.getR()).build();
                    } else if (pair.getL().equals("Boolean")) {
                        return new FieldValue.Builder().booleanValue((Boolean) pair.getR()).build();
                    }

                    return null;
                }
            };
            var module = new SimpleModule()
                .addDeserializer(FieldValue.class, fieldValueDeserializer)
                .addSerializer(FieldValue.class, fieldValueSerializer);

            this.objectMapper.registerModule(module);
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
