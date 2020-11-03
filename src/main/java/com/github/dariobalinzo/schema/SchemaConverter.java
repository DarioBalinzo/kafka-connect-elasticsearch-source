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
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.List;
import java.util.Map;

public class SchemaConverter {

    public Schema convert(Map<String, Object> elasticDocument, String schemaName) {
        SchemaBuilder schemaBuilder = SchemaBuilder.struct()
                .name(
                        AvroName.from("", schemaName)
                );
        convertDocumentSchema("", elasticDocument, schemaBuilder);
        return schemaBuilder.build();

    }

    private void convertDocumentSchema(String prefixName, Map<String, Object> doc, SchemaBuilder schemaBuilder) {
        for (Map.Entry<String, Object> entry : doc.entrySet()) {
            String key = entry.getKey();
            Object value = entry.getValue();
            if (value instanceof String) {
                schemaBuilder.field(AvroName.from(key), Schema.OPTIONAL_STRING_SCHEMA);
            } else if (value instanceof Integer) {
                schemaBuilder.field(AvroName.from(key), Schema.OPTIONAL_INT32_SCHEMA);
            } else if (value instanceof Long) {
                schemaBuilder.field(AvroName.from(key), Schema.OPTIONAL_INT64_SCHEMA);
            } else if (value instanceof Float) {
                schemaBuilder.field(AvroName.from(key), Schema.OPTIONAL_FLOAT32_SCHEMA);
            } else if (value instanceof Double) {
                schemaBuilder.field(AvroName.from(key), Schema.OPTIONAL_FLOAT64_SCHEMA);
            } else if (value instanceof List) {
                if (!((List<?>) value).isEmpty()) {
                    //assuming that every item of the list has the same schema
                    Object head = ((List<?>) value).get(0);
                    convertListSchema(prefixName, schemaBuilder, key, head);
                }
            } else if (value instanceof Map) {
                convertMapSchema(prefixName, schemaBuilder, entry);
            } else {
                throw new RuntimeException("type not supported " + key);
            }
        }

    }

    @SuppressWarnings("unchecked")
    private void convertMapSchema(String prefixName, SchemaBuilder schemaBuilder, Map.Entry<String, Object> entry) {
        String key = entry.getKey();
        Map<String, Object> value = (Map<String, Object>) entry.getValue();
        SchemaBuilder nestedSchema = SchemaBuilder.struct().name(AvroName.from(prefixName, key)).optional();
        convertDocumentSchema(
                AvroName.from(prefixName, key) + ".",
                value,
                nestedSchema
        );
        schemaBuilder.field(AvroName.from(key), nestedSchema.build());
    }

    @SuppressWarnings("unchecked")
    private void convertListSchema(String prefixName, SchemaBuilder schemaBuilder, String k, Object item) {
        if (item instanceof String) {
            schemaBuilder.field(
                    AvroName.from(k),
                    SchemaBuilder.array(SchemaBuilder.OPTIONAL_STRING_SCHEMA)
                            .optional()
                            .build()
            ).build();
        } else if (item instanceof Integer) {
            schemaBuilder.field(
                    AvroName.from(k),
                    SchemaBuilder.array(SchemaBuilder.OPTIONAL_INT32_SCHEMA)
                            .optional()
                            .build()
            ).build();
        } else if (item instanceof Long) {
            schemaBuilder.field(AvroName.from(k),
                    SchemaBuilder.array(SchemaBuilder.OPTIONAL_INT64_SCHEMA)
                            .optional()
                            .build()
            ).build();
        } else if (item instanceof Float) {
            schemaBuilder.field(AvroName.from(k),
                    SchemaBuilder.array(SchemaBuilder.OPTIONAL_FLOAT32_SCHEMA)
                            .optional()
                            .build()
            ).build();
        } else if (item instanceof Double) {
            schemaBuilder.field(AvroName.from(k),
                    SchemaBuilder.array(SchemaBuilder.OPTIONAL_FLOAT64_SCHEMA)
                            .optional()
                            .build()
            ).build();
        } else if (item instanceof Map) {
            SchemaBuilder nestedSchema = SchemaBuilder.struct()
                    .name(AvroName.from(prefixName, k))
                    .optional();
            convertDocumentSchema(AvroName.from(prefixName, k) + ".",
                    (Map<String, Object>) item,
                    nestedSchema);
            schemaBuilder.field(AvroName.from(k), SchemaBuilder.array(nestedSchema.build()));
        } else {
            throw new RuntimeException("error in converting list: type not supported");
        }
    }


}
