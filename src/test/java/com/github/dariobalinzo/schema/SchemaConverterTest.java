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


import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.data.Struct;
import org.junit.Assert;
import org.junit.Test;

import java.util.*;

public class SchemaConverterTest {

    private final SchemaConverter schemaConverter = new ParsingSchemaConverter(new AvroName());
    private final StructConverter structConverter = new ParsingStructConverter(new AvroName());
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void shouldConvertSimpleSchema() {
        //given
        Map<String, Object> elasticDocument = new LinkedHashMap<>();
        elasticDocument.put("name", "elastic");
        elasticDocument.put("surname", "search");
        elasticDocument.put("version", 7);
        elasticDocument.put("enabled", true);

        //when
        Schema schema = schemaConverter.convert(elasticDocument, "test");
        Struct struct = structConverter.convert(elasticDocument, schema);

        //then
        Assert.assertEquals("[" +
                        "Field{name=name, index=0, schema=Schema{STRING}}," +
                        " Field{name=surname, index=1, schema=Schema{STRING}}, " +
                        "Field{name=version, index=2, schema=Schema{INT64}}, " +
                        "Field{name=enabled, index=3, schema=Schema{BOOLEAN}}" +
                        "]",
                schema.fields().toString()
        );
        Assert.assertEquals("Struct{name=elastic,surname=search,version=7,enabled=true}", struct.toString());
    }

    @Test
    public void shouldConvertNullValues() {
        //given
        Map<String, Object> elasticDocument = new LinkedHashMap<>();
        elasticDocument.put("name", "elastic");
        elasticDocument.put("surname", null);
        elasticDocument.put("version", 7);
        elasticDocument.put("enabled", true);

        //when
        Schema schema = schemaConverter.convert(elasticDocument, "test");
        Struct struct = structConverter.convert(elasticDocument, schema);

        //then
        Assert.assertEquals("[" +
                        "Field{name=name, index=0, schema=Schema{STRING}}, " +
                        "Field{name=version, index=1, schema=Schema{INT64}}, " +
                        "Field{name=enabled, index=2, schema=Schema{BOOLEAN}}" +
                        "]",
                schema.fields().toString()
        );
        Assert.assertEquals("Struct{name=elastic,version=7,enabled=true}", struct.toString());
    }

    @Test(expected = RuntimeException.class)
    public void shouldRejectInvalidDocuments() {
        Map<String, Object> invalidDoc = new HashMap<>();
        invalidDoc.put("not_supported", new NotSupported());
        schemaConverter.convert(invalidDoc, "test");
    }

    @Test(expected = RuntimeException.class)
    public void shouldRejectInvalidDocumentsInStructs() {
        Map<String, Object> invalidDoc = new HashMap<>();
        invalidDoc.put("not_supported", new NotSupported());
        structConverter.convert(invalidDoc, new SchemaBuilder(Schema.Type.STRUCT).schema());
    }

    @Test
    public void shouldConvertNestedObject() {
        //given
        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("foo", "bar");

        Map<String, Object> elasticDocument = new LinkedHashMap<>();
        elasticDocument.put("name", "elastic");
        elasticDocument.put("version", 7);
        elasticDocument.put("detail", nested);

        //when
        Schema schema = schemaConverter.convert(elasticDocument, "test");
        Struct struct = structConverter.convert(elasticDocument, schema);

        //then
        Assert.assertEquals("[" +
                        "Field{name=name, index=0, schema=Schema{STRING}}," +
                        " Field{name=version, index=1, schema=Schema{INT64}}," +
                        " Field{name=detail, index=2, schema=Schema{detail:STRUCT}}" +
                        "]",
                schema.fields().toString()
        );

        Assert.assertEquals("[Field{name=foo, index=0, schema=Schema{STRING}}]",
                schema.field("detail")
                        .schema()
                        .fields()
                        .toString()
        );
        Assert.assertEquals("Struct{name=elastic,version=7,detail=Struct{foo=bar}}", struct.toString());
    }

    @Test
    public void shouldConvertLists() {
        //given
        Map<String, Object> elasticDocument = new LinkedHashMap<>();
        elasticDocument.put("name", "elastic");
        elasticDocument.put("details", Arrays.asList(1, 2, 3));

        //when
        Schema schema = schemaConverter.convert(elasticDocument, "test");
        Struct struct = structConverter.convert(elasticDocument, schema);

        //then
        Assert.assertEquals("[" +
                        "Field{name=name, index=0, schema=Schema{STRING}}, " +
                        "Field{name=details, index=1, schema=Schema{ARRAY}}" +
                        "]",
                schema.fields().toString()
        );

        Assert.assertEquals("Schema{INT64}",
                schema.field("details")
                        .schema()
                        .valueSchema()
                        .toString()
        );
        Assert.assertEquals("Struct{name=elastic,details=[1, 2, 3]}", struct.toString());
    }

    @Test
    public void shouldConvertListsOfObject() {
        //given
        Map<String, Object> nested = new LinkedHashMap<>();
        nested.put("foo", "bar");

        Map<String, Object> elasticDocument = new LinkedHashMap<>();
        elasticDocument.put("name", "elastic");
        elasticDocument.put("details", Collections.singletonList(nested));

        //when
        Schema schema = schemaConverter.convert(elasticDocument, "test");
        Struct struct = structConverter.convert(elasticDocument, schema);

        //then
        Assert.assertEquals("Struct{name=elastic,details=[Struct{foo=bar}]}", struct.toString());
    }

    private static class NotSupported {
    }
}
