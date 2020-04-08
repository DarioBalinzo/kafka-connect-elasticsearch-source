/**
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

import com.github.dariobalinzo.utils.Utils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.List;
import java.util.Map;

public class SchemaConverter {

    public static Schema convertElasticMapping2AvroSchema(Map<String, Object> doc, String name) {
        SchemaBuilder schemaBuilder = SchemaBuilder.struct().name(
                Utils.filterAvroName("", name)); //characters not valid for avro schema name
        convertDocumentSchema("", doc, schemaBuilder);
        return schemaBuilder.build();
    }

    private static void convertDocumentSchema(String prefixName, Map<String, Object> doc, SchemaBuilder schemaBuilder) {

        doc.keySet().forEach(
                k -> {
                    Object v = doc.get(k);
                    if (v instanceof String) {
                        schemaBuilder.field(Utils.filterAvroName(k), Schema.OPTIONAL_STRING_SCHEMA);
                    } else if (v instanceof Integer) {
                        schemaBuilder.field(Utils.filterAvroName(k), Schema.OPTIONAL_INT32_SCHEMA);
                    } else if (v instanceof Long) {
                        schemaBuilder.field(Utils.filterAvroName(k), Schema.OPTIONAL_INT64_SCHEMA);
                    } else if (v instanceof Float) {
                        schemaBuilder.field(Utils.filterAvroName(k), Schema.OPTIONAL_FLOAT32_SCHEMA);
                    } else if (v instanceof Double) {
                        schemaBuilder.field(Utils.filterAvroName(k), Schema.OPTIONAL_FLOAT64_SCHEMA);
                    } else if (v instanceof Boolean) {
                        schemaBuilder.field(Utils.filterAvroName(k), Schema.OPTIONAL_BOOLEAN_SCHEMA);
                    }
                    else if (v instanceof List) {

                        if (!((List) v).isEmpty()) {
                            //assuming that every item of the list has the same schema
                            Object item = ((List) v).get(0);
                            if (item instanceof String) {
                                schemaBuilder.field(Utils.filterAvroName(k), SchemaBuilder.array(SchemaBuilder.OPTIONAL_STRING_SCHEMA)
                                        .optional()
                                        .build()
                                ).build();
                            } else if (item instanceof Integer) {
                                schemaBuilder.field(Utils.filterAvroName(k), SchemaBuilder.array(SchemaBuilder.OPTIONAL_INT32_SCHEMA)
                                        .optional()
                                        .build()
                                ).build();
                            } else if (item instanceof Long) {
                                schemaBuilder.field(Utils.filterAvroName(k), SchemaBuilder.array(SchemaBuilder.OPTIONAL_INT64_SCHEMA)
                                        .optional()
                                        .build()
                                ).build();
                            } else if (item instanceof Float) {
                                schemaBuilder.field(Utils.filterAvroName(k), SchemaBuilder.array(SchemaBuilder.OPTIONAL_FLOAT32_SCHEMA)
                                        .optional()
                                        .build()
                                ).build();
                            }
                            if (item instanceof Double) {
                                schemaBuilder.field(Utils.filterAvroName(k), SchemaBuilder.array(SchemaBuilder.OPTIONAL_FLOAT64_SCHEMA)
                                        .optional()
                                        .build()
                                ).build();
                            } else if (item instanceof Map) {

                                SchemaBuilder nestedSchema = SchemaBuilder.struct()
                                        .name(Utils.filterAvroName(prefixName, k))
                                        .optional();
                                convertDocumentSchema(Utils.filterAvroName(prefixName, k) + ".",
                                        (Map<String, Object>) item,
                                        nestedSchema);
                                schemaBuilder.field(Utils.filterAvroName(k), SchemaBuilder.array(nestedSchema.build()));
                            } else {
                                throw new RuntimeException("error in converting list: type not supported");
                            }
                        }

                    } else if (v instanceof Map) {

                        SchemaBuilder nestedSchema = SchemaBuilder.struct().name(Utils.filterAvroName(prefixName, k)).optional();
                        convertDocumentSchema(Utils.filterAvroName(prefixName, k) + ".",
                                (Map<String, Object>) v,
                                nestedSchema
                        );
                        schemaBuilder.field(Utils.filterAvroName(k), nestedSchema.build());

                    } else {
                        throw new RuntimeException("type not supported " + k);
                    }
                }
        );
    }
}
