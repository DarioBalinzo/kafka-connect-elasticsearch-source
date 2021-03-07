/*
 * Copyright © 2018 Dario Balinzo (dariobalinzo@gmail.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.dariobalinzo.schema;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class StructConverter {

    private final FieldNameConverter converter;

    public StructConverter(FieldNameConverter converter) {
        this.converter = converter;
    }

    public Struct convert(Map<String, Object> doc, Schema schema) {
        Struct struct = new Struct(schema);
        convertDocumentStruct("", doc, struct, schema);
        return struct;
    }

    private void convertDocumentStruct(String prefixName, Map<String, Object> doc, Struct struct, Schema schema) {
        for (Map.Entry<String, Object> entry : doc.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();

            if (isScalar(value)) {
                value = handleNumericPrecision(value);
                struct.put(converter.from(key), value);
            } else if (value instanceof List) {
                convertListToAvroArray(prefixName, struct, schema, entry);
            } else if (value instanceof Map) {
                covertMapToAvroStruct(prefixName, struct, schema, entry);
            } else {
                if (value != null) {
                    throw new RuntimeException("type not supported " + key);
                }
            }
        }
    }

    private boolean isScalar(Object value) {
        return value instanceof String
                || value instanceof Boolean
                || value instanceof Integer
                || value instanceof Long
                || value instanceof Double
                || value instanceof Float;
    }

    private Object handleNumericPrecision(Object value) {
        if (value instanceof Integer) {
            value = ((Integer) value).longValue();
        } else if (value instanceof Float) {
            value = ((Float) value).doubleValue();
        }
        return value;
    }

    @SuppressWarnings("unchecked")
    private void convertListToAvroArray(String prefixName, Struct struct, Schema schema, Map.Entry<String, Object> entry) {
        String key = entry.getKey();
        List<?> value = (List<?>) entry.getValue();

        if (!value.isEmpty()) {
            //assuming that every item of the list has the same schema
            Object head = value.get(0);
            struct.put(converter.from(key), new ArrayList<>());
            if (isScalar(head)) {
                List<Object> scalars = value.stream()
                        .map(this::handleNumericPrecision)
                        .collect(Collectors.toList());
                struct.getArray(converter.from(key)).addAll(scalars);
            } else if (head instanceof Map) {
                List<Struct> array = value
                        .stream()
                        .map(doc -> convertListOfObject(prefixName, schema, key, (Map<String, Object>) doc))
                        .collect(Collectors.toCollection(ArrayList::new));
                struct.put(converter.from(key), array);
            } else {
                throw new RuntimeException("error in converting list: type not supported");
            }

        }
    }

    @SuppressWarnings("unchecked")
    private void covertMapToAvroStruct(String prefixName, Struct struct, Schema schema, Map.Entry<String, Object> entry) {
        String k = entry.getKey();
        Map<String, Object> value = (Map<String, Object>) entry.getValue();
        Struct nestedStruct = new Struct(schema.field(converter.from(k)).schema());
        convertDocumentStruct(
                converter.from(prefixName, k) + ".",
                value,
                nestedStruct,
                schema.field(converter.from(k)).schema()
        );
        struct.put(converter.from(k), nestedStruct);
    }

    private Struct convertListOfObject(String prefixName, Schema schema, String key, Map<String, Object> doc) {
        String validKey = converter.from(key);
        String validKeyPrefix = converter.from(prefixName, key) + ".";
        Struct nestedStruct = new Struct(
                schema.field(validKey)
                        .schema()
                        .valueSchema()
        );

        convertDocumentStruct(
                validKeyPrefix,
                doc,
                nestedStruct,
                schema.field(validKey)
                        .schema()
                        .valueSchema()
        );
        return nestedStruct;
    }


}
