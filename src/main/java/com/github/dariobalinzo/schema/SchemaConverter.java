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

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.List;
import java.util.Map;

public class SchemaConverter {

    public static Schema convertElasticMapping2AvroSchema(Map<String, Object> doc, String name) {

        SchemaBuilder schemaBuilder = SchemaBuilder.struct().name(
                name.replace(".", "").replace("-", "")); //characters not valid for avro schema name
        convertDocumentSchema(doc,schemaBuilder);
        return schemaBuilder.build();

    }


    private static void convertDocumentSchema(Map<String, Object> doc, SchemaBuilder schemaBuilder) {

        doc.keySet().forEach(
                k -> {
                    Object v = doc.get(k);
                    if (v instanceof String) {
                        schemaBuilder.field(k, Schema.OPTIONAL_STRING_SCHEMA);
                    } else if (v instanceof Integer ) {
                        schemaBuilder.field(k, Schema.OPTIONAL_INT32_SCHEMA);
                    } else if (v instanceof Long) {
                        schemaBuilder.field(k, Schema.OPTIONAL_INT64_SCHEMA);
                    } else if ( v instanceof Float) {
                        schemaBuilder.field(k, Schema.OPTIONAL_FLOAT32_SCHEMA);
                    } else if (v instanceof Double) {
                        schemaBuilder.field(k, Schema.OPTIONAL_FLOAT64_SCHEMA);
                    } else if (v instanceof List) {

                        if (!((List) v).isEmpty()) {
                            //assuming that every item of the list has the same schema
                            Object item = ((List) v).get(0);
                            if (item instanceof String) {
                                schemaBuilder.field(k, SchemaBuilder.array(SchemaBuilder.OPTIONAL_STRING_SCHEMA)
                                        .build()
                                ).build();
                            } else if (item instanceof Integer || item instanceof Long) {
                                schemaBuilder.field(k, SchemaBuilder.array(SchemaBuilder.OPTIONAL_INT32_SCHEMA)
                                        .build()
                                ).build();
                            } else if (item instanceof Double || item instanceof Float) {
                                schemaBuilder.field(k, SchemaBuilder.array(SchemaBuilder.OPTIONAL_FLOAT64_SCHEMA)
                                        .build()
                                ).build();
                            } else if (item instanceof Map) {

                                SchemaBuilder nestedSchema = SchemaBuilder.struct().name(k);
                                convertDocumentSchema((Map<String, Object>) item, nestedSchema);
                                schemaBuilder.field(k, SchemaBuilder.array(nestedSchema.build()));
                            } else {
                                throw new RuntimeException("error in converting list: type not supported");
                            }

                        }

                    } else if (v instanceof Map) {

                        SchemaBuilder nestedSchema = SchemaBuilder.struct().name(k);
                        convertDocumentSchema((Map<String, Object>) v, nestedSchema);
                        schemaBuilder.field(k, nestedSchema.build());

                    } else {
                        throw new RuntimeException("type not supported "+k);
                    }
                }
        );

    }


}
