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
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class SchemaConverterTest {

    private final SchemaConverter schemaConverter = new SchemaConverter();

    @Test
    public void shouldConvertSimpleSchema() {
        //given
        Map<String, Object> elasticDocument = new HashMap<>();
        elasticDocument.put("name", "elastic");
        elasticDocument.put( "surname", "search");
        elasticDocument.put("version", 7);

        //when
        Schema schema = schemaConverter.convertToAvro(elasticDocument, "test");

        //then
        Assert.assertEquals("[" +
                    "Field{name=versione, index=0, schema=Schema{INT32}}," +
                    " Field{name=surname, index=1, schema=Schema{STRING}}," +
                    " Field{name=name, index=2, schema=Schema{STRING}}" +
                "]",
                schema.fields().toString()
        );
    }

    @Test
    public void shouldConvertNestedObject() {
        //given
        Map<String, Object> nested = new HashMap<>();
        nested.put("foo", "bar");

        Map<String, Object> elasticDocument = new HashMap<>();
        elasticDocument.put("name", "elastic");
        elasticDocument.put("version", 7);
        elasticDocument.put("detail", nested);

        //when
        Schema schema = schemaConverter.convertToAvro(elasticDocument, "test");

        //then
        Assert.assertEquals("[" +
                            "Field{name=name, index=0, schema=Schema{STRING}}, " +
                            "Field{name=detail, index=1, schema=Schema{detail:STRUCT}}, " +
                            "Field{name=version, index=2, schema=Schema{INT32}}" +
                        "]",
                schema.fields().toString()
        );

        Assert.assertEquals("[Field{name=foo, index=0, schema=Schema{STRING}}]",
                schema.field("detail")
                        .schema()
                        .fields()
                        .toString()
        );
    }

    @Test
    public void shouldConvertLists() {
        //given
        Map<String, Object> elasticDocument = new HashMap<>();
        elasticDocument.put("name", "elastic");
        elasticDocument.put("details", Arrays.asList(1, 2, 3));

        //when
        Schema schema = schemaConverter.convertToAvro(elasticDocument, "test");

        //then
        Assert.assertEquals("[" +
                            "Field{name=name, index=0, schema=Schema{STRING}}, " +
                            "Field{name=details, index=1, schema=Schema{ARRAY}}" +
                        "]",
                schema.fields().toString()
        );

        Assert.assertEquals("Schema{INT32}",
                schema.field("details")
                        .schema()
                        .valueSchema()
                        .toString()
        );
    }

    @Test
    public void shouldConvertListsOfObject() {
        //given
        Map<String, Object> nested = new HashMap<>();
        nested.put("foo", "bar");

        Map<String, Object> elasticDocument = new HashMap<>();
        elasticDocument.put("name", "elastic");
        elasticDocument.put("details", nested);

        //when
        Schema schema = schemaConverter.convertToAvro(elasticDocument, "test");

        //then
        Assert.assertEquals("[" +
                            "Field{name=name, index=0, schema=Schema{STRING}}, " +
                            "Field{name=details, index=1, schema=Schema{details:STRUCT}}" +
                        "]",
                schema.fields().toString()
        );

        Assert.assertEquals("[Field{name=foo, index=0, schema=Schema{STRING}}]",
                schema.field("details")
                        .schema()
                        .fields()
                        .toString()
        );
    }


}
