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

import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.kafka.connect.data.Schema.*;
import static org.apache.kafka.connect.data.SchemaBuilder.array;
import static org.apache.kafka.connect.data.SchemaBuilder.struct;

public class SchemaConverter {

    private final FieldNameConverter converter;

    public SchemaConverter(FieldNameConverter converter) {
        this.converter = converter;
    }

    public Schema convert(Map<String, Object> elasticDocument, String schemaName) {
        String validSchemaName = converter.from("", schemaName);
        SchemaBuilder schemaBuilder = struct().name(validSchemaName);
        convertDocumentSchema("", elasticDocument, schemaBuilder);
        return schemaBuilder.build();
    }

    @SuppressWarnings("unchecked")
    private void convertDocumentSchema(String prefixName, Map<String, Object> doc, SchemaBuilder schemaBuilder) {
        for (Map.Entry<String, Object> entry : doc.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            String validKeyName = converter.from(key);
            if (value instanceof String) {
                schemaBuilder.field(validKeyName, OPTIONAL_STRING_SCHEMA);
            } else if (value instanceof Boolean) {
                schemaBuilder.field(validKeyName, OPTIONAL_BOOLEAN_SCHEMA);
            } else if (value instanceof Integer) {
                schemaBuilder.field(validKeyName, OPTIONAL_INT64_SCHEMA);
            } else if (value instanceof Long) {
                schemaBuilder.field(validKeyName, OPTIONAL_INT64_SCHEMA);
            } else if (value instanceof Float) {
                schemaBuilder.field(validKeyName, OPTIONAL_FLOAT64_SCHEMA);
            } else if (value instanceof Double) {
                schemaBuilder.field(validKeyName, OPTIONAL_FLOAT64_SCHEMA);
            } else if (value instanceof List) {
                if (!((List<?>) value).isEmpty()) {
                    Object head = ((List<?>) value).get(0);
                    if (head instanceof Map) {
                        convertListOfObject(prefixName, schemaBuilder, key, (List<Map<String, Object>>) value);
                    } else {
                        convertListSchema(prefixName, schemaBuilder, key, head);
                    }
                }
            } else if (value instanceof Map) {
                convertMapSchema(prefixName, schemaBuilder, entry);
            } else {
                if (value != null) {
                    throw new RuntimeException("type not supported " + key);
                }
            }
        }
    }

    @SuppressWarnings("unchecked")
    private void convertMapSchema(String prefixName, SchemaBuilder schemaBuilder, Map.Entry<String, Object> entry) {
        String key = entry.getKey();
        Map<String, Object> value = (Map<String, Object>) entry.getValue();
        String validKeyName = converter.from(prefixName, key);
        SchemaBuilder nestedSchema = struct().name(validKeyName).optional();
        convertDocumentSchema(validKeyName + ".", value, nestedSchema);
        schemaBuilder.field(converter.from(key), nestedSchema.build());
    }

    @SuppressWarnings("unchecked")
    private void convertListSchema(String prefixName, SchemaBuilder schemaBuilder, String k, Object item) {
        String validKeyName = converter.from(k);
        if (item instanceof String) {
            schemaBuilder.field(
                    validKeyName,
                    array(OPTIONAL_STRING_SCHEMA).optional().build()
            ).build();
        } else if (item instanceof Boolean) {
            schemaBuilder.field(
                    validKeyName,
                    array(OPTIONAL_BOOLEAN_SCHEMA).optional().build()
            ).build();
        } else if (item instanceof Integer) {
            schemaBuilder.field(
                    validKeyName,
                    array(OPTIONAL_INT64_SCHEMA).optional().build()
            ).build();
        } else if (item instanceof Long) {
            schemaBuilder.field(
                    validKeyName,
                    array(OPTIONAL_INT64_SCHEMA).optional().build()
            ).build();
        } else if (item instanceof Float) {
            schemaBuilder.field(
                    validKeyName,
                    array(OPTIONAL_FLOAT64_SCHEMA).optional().build()
            ).build();
        } else if (item instanceof Double) {
            schemaBuilder.field(
                    validKeyName,
                    array(OPTIONAL_FLOAT64_SCHEMA).optional().build()
            ).build();
        } else {
            throw new RuntimeException("error in converting list: type not supported");
        }
    }


    private void convertListOfObject(String prefixName, SchemaBuilder schemaBuilder, String k, List<Map<String, Object>> list) {
        String validKeyName = converter.from(k);
        String keyWithPrefix = converter.from(prefixName, k);
        Map<String, Field> fieldsUnion = new HashMap<>();
        for (Map<String, Object> obj : list) {
            SchemaBuilder nestedSchema = struct().name(keyWithPrefix).optional();
            convertDocumentSchema(keyWithPrefix + ".", obj, nestedSchema);
            for (Field field : nestedSchema.fields()) {
                fieldsUnion.putIfAbsent(field.name(), field);
            }
        }
        SchemaBuilder unionNestedSchema = struct().name(prefixName).optional();
        for (Field field : fieldsUnion.values()) {
            unionNestedSchema.field(field.name(), field.schema());
        }
        schemaBuilder.field(validKeyName, array(unionNestedSchema.build()));
    }


}
