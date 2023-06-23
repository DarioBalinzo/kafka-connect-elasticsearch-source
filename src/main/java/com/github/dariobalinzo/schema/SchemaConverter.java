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

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import static org.apache.kafka.connect.data.Schema.OPTIONAL_BOOLEAN_SCHEMA;
import static org.apache.kafka.connect.data.Schema.OPTIONAL_FLOAT64_SCHEMA;
import static org.apache.kafka.connect.data.Schema.OPTIONAL_INT64_SCHEMA;
import static org.apache.kafka.connect.data.Schema.OPTIONAL_STRING_SCHEMA;
import static org.apache.kafka.connect.data.Schema.Type.STRUCT;
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
                        convertListSchema(prefixName, schemaBuilder, key, (List<?>)value);
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
    private void convertListSchema(String prefixName, SchemaBuilder schemaBuilder, String k, List<?> items) {
        String validKeyName = converter.from(k);

        Set<Schema> schemas = items.stream().filter(i -> i != null).map(this::convertListSchema).collect(Collectors.toSet());
        Schema itemSchema;
        if(schemas.isEmpty()) {
            itemSchema = OPTIONAL_STRING_SCHEMA;
        } else if(schemas.size() == 1) {
            itemSchema = schemas.iterator().next();
        } else if(!schemas.contains(OPTIONAL_STRING_SCHEMA) && !schemas.contains(OPTIONAL_BOOLEAN_SCHEMA)) {
            itemSchema = OPTIONAL_FLOAT64_SCHEMA;
        } else {
            throw new IllegalArgumentException("list " + validKeyName + " contains items of different schemas: " + schemas);
        }

        schemaBuilder.field(
                validKeyName,
                array(itemSchema).optional().build()
        ).build();
    }

    private Schema convertListSchema(Object item) {
        if (item instanceof String) {
            return OPTIONAL_STRING_SCHEMA;
        } else if (item instanceof Boolean) {
            return OPTIONAL_BOOLEAN_SCHEMA;
        } else if (item instanceof Integer) {
            return OPTIONAL_INT64_SCHEMA;
        } else if (item instanceof Long) {
            return OPTIONAL_INT64_SCHEMA;
        } else if (item instanceof Float) {
            return OPTIONAL_FLOAT64_SCHEMA;
        } else if (item instanceof Double) {
            return OPTIONAL_FLOAT64_SCHEMA;
        } else {
            throw new RuntimeException("error in converting list: type not supported " + item.getClass());
        }
    }


    private void convertListOfObject(String prefixName, SchemaBuilder schemaBuilder, String k,
                                     List<Map<String, Object>> list) {
        String validKeyName = converter.from(k);
        String keyWithPrefix = converter.from(prefixName, k);
        Schema current = null;
        for (Map<String, Object> obj : list) {
            SchemaBuilder nestedSchema = struct().name(keyWithPrefix).optional();
            convertDocumentSchema(keyWithPrefix + ".", obj, nestedSchema);

            if(current == null) {
                current = nestedSchema;
            } else {
                current = merge(current,  nestedSchema);
            }
        }
        schemaBuilder.field(validKeyName, array(current));
    }

    private Schema merge(Schema a, Schema b) {
        if (!(a.type() == STRUCT && b.type() == STRUCT)) {
            // we can only merge structs, we therefor always return the first found schema
            return a;
        }

        Map<String, Schema> fieldsUnion = new LinkedHashMap<>();
        Consumer<Field> collector = f -> {
            fieldsUnion.computeIfPresent(f.name(), (key, old) -> merge(old.schema(), f.schema()));
            fieldsUnion.putIfAbsent(f.name(), f.schema());
        };
        a.fields().forEach(collector);
        b.fields().forEach(collector);

        SchemaBuilder union = struct().name(a.name()).optional();
        for (Map.Entry<String, Schema> field : fieldsUnion.entrySet()) {
            union.field(field.getKey(), from(field.getValue()).optional().build());
        }
        return union;
    }

    private SchemaBuilder from(Schema schema) {
        if(schema instanceof SchemaBuilder) {
            return (SchemaBuilder) schema;
        } else {
            SchemaBuilder builder;
            switch (schema.type()) {
                case STRUCT: {
                    builder = struct();
                    for (Field field : schema.fields()) {
                        builder.field(field.name(), field.schema());
                    }
                    break;
                }
                case MAP: {
                    builder = SchemaBuilder.map(schema.keySchema(), schema.valueSchema());
                    break;
                }
                case ARRAY: {
                    builder = SchemaBuilder.array(schema.valueSchema());
                    break;
                }
                default: {
                    builder = new SchemaBuilder(schema.type());
                    break;
                }
            }
            if(schema.isOptional()) {
                builder.optional();
            }
            builder.name(schema.name());
            if(schema.defaultValue() != null) {
                builder.defaultValue(schema.defaultValue());
            }
            builder.doc(schema.doc());
            if(schema.parameters() != null) {
                builder.parameters(schema.parameters());
            }
            builder.version(schema.version());
            return builder;
        }
    }
}
