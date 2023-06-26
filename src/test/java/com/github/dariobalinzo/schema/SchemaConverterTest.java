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

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonList;
import static org.apache.kafka.connect.data.SchemaBuilder.string;
import static org.apache.kafka.connect.data.SchemaBuilder.struct;

public class SchemaConverterTest {

    private final SchemaConverter schemaConverter = new SchemaConverter(new AvroName());
    private final StructConverter structConverter = new StructConverter(new AvroName());
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void shouldConvertArrayWithDifferentSchema() {
        //given
        Map<String, Object> elasticDocument = mapOf(
                "list", asList(
                        mapOf("inner", mapOf("a", "some value")),
                        mapOf("inner", mapOf("b", "some value"))
                )
        );

        //when
        Schema schema = schemaConverter.convert(elasticDocument, "test");
        Struct struct = structConverter.convert(elasticDocument, schema);

        //then
        Schema expected = struct().name("list.inner")
                                  .optional()
                                  .field("a", string().optional().build())
                                  .field("b", string().optional().build())
                                  .build();
        Schema innerSchema = schema.field("list").schema().valueSchema().field("inner").schema().schema();
        Assert.assertEquals(expected, innerSchema);
        Assert.assertEquals("Struct{list=[Struct{inner=Struct{a=some value}}, Struct{inner=Struct{b=some value}}]}", struct.toString());
    }
    @Test
    public void shouldConvertArrayWithDifferentSchemaAlsoWhenDeep() {
        //given
        Map<String, Object> elasticDocument = mapOf(
                "out", singletonList(
                        mapOf(
                                "list", asList(
                                        mapOf("inner", mapOf("a", "some value")),
                                        mapOf("inner", mapOf("b", "some value"))
                                )
                        )
                )
        );

        //when
        Schema schema = schemaConverter.convert(elasticDocument, "test");
        Struct struct = structConverter.convert(elasticDocument, schema);

        //then
        Schema expected = struct().name("out.list.inner")
                                  .optional()
                                  .field("a", string().optional().build())
                                  .field("b", string().optional().build())
                                  .build();
        Schema innerSchema = schema.field("out").schema()
                                   .valueSchema()
                                   .field("list").schema()
                                   .valueSchema()
                                   .field("inner").schema().schema();
        Assert.assertEquals(expected, innerSchema);
        Assert.assertEquals("Struct{out=[Struct{list=[Struct{inner=Struct{a=some value}}, Struct{inner=Struct{b=some value}}]}]}", struct.toString());
    }

    @Test
    public void shouldConvertNormalListsWithoutMerging() {
        //given
        Map<String, Object> elasticDocument = mapOf(
                "list", asList(
                        mapOf("inner", 3),
                        mapOf("inner", 4)
                )
        );

        //when
        Schema schema = schemaConverter.convert(elasticDocument, "test");
        Struct struct = structConverter.convert(elasticDocument, schema);

        //then
        Schema innerSchema = schema.field("list").schema().valueSchema().field("inner").schema().schema();
        Assert.assertEquals("Schema{INT64}", innerSchema.toString());
        Assert.assertEquals("Struct{list=[Struct{inner=3}, Struct{inner=4}]}", struct.toString());
    }

    private Map<String, Object> mapOf(String key, Object value) {
        Map<String, Object> map = new LinkedHashMap<>();
        map.put(key, value);
        return map;
    }

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
        elasticDocument.put("details", asList(1, 2, 3));

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
        elasticDocument.put("details", singletonList(nested));

        //when
        Schema schema = schemaConverter.convert(elasticDocument, "test");
        Struct struct = structConverter.convert(elasticDocument, schema);

        //then
        Assert.assertEquals("Struct{name=elastic,details=[Struct{foo=bar}]}", struct.toString());
    }

    @Test
    public void shouldConvertComplexNestedJson() throws IOException {
        //given
        String file = this.getClass().getClassLoader()
                .getResource("com/github/dariobalinzo/schema/complexDocument.json")
                .getFile();
        String jsonDocument = new String(Files.readAllBytes(Paths.get(file)));

        Map<String, Object> elasticDocument = objectMapper.readValue(jsonDocument, Map.class);

        //when
        Schema schema = schemaConverter.convert(elasticDocument, "test");
        Struct struct = structConverter.convert(elasticDocument, schema);

        //then
        Assert.assertEquals("Struct{" +
                "currenttime=2020-12-11T07:24:44Z,ip=192.168.1.111," +
                "xxxapiendpoint=https://192.168.1.111:5001/api,parent=xxx/d0cee1fb-8fc8-427a-88de-ab4f7b2f8b8b," +
                "architecture=armv7l,updated=2020-12-11T07:24:45.198Z," +
                "lastboot=2020-12-09T21:45:27Z," +
                "xxxengineversion=1.13.0," +
                "dockerserverversion=19.03.14," +
                "created=2020-12-10T08:06:38.652Z," +
                "hostname=xyz," +
                "updatedby=xxx/d0cee1fb-8fc8-427a-88de-ab4f7b2f8b8b," +
                "gpiopins=[" +
                "Struct{pin=1,name=3.3v}, " +
                "Struct{pin=2,name=5v}, " +
                "Struct{pin=3,name=SDA.1,bcm=2,mode=IN,voltage=1}, " +
                "Struct{pin=4,name=5v}, " +
                "Struct{pin=5,name=SCL.1,bcm=3,mode=IN,voltage=1}, " +
                "Struct{pin=6,name=0v}, Struct{pin=7,name=GPIO. 7,bcm=4,mode=IN,voltage=1}, " +
                "Struct{pin=8,name=TxD,mode=IN,voltage=1}, Struct{pin=9,name=0v}, " +
                "Struct{pin=10,name=RxD,mode=IN,voltage=1}, Struct{pin=11,name=GPIO. 0,bcm=17,mode=IN,voltage=0}, " +
                "Struct{pin=12,name=GPIO. 1,mode=IN,voltage=0}, Struct{pin=13,name=GPIO. 2,bcm=27,mode=IN,voltage=0}, " +
                "Struct{pin=14,name=0v}, Struct{pin=15,name=GPIO. 3,bcm=22,mode=IN,voltage=0}, " +
                "Struct{pin=16,name=GPIO. 4,mode=IN,voltage=0}, " +
                "Struct{pin=17,name=3.3v}, " +
                "Struct{pin=18,name=GPIO. 5,mode=IN,voltage=0}, " +
                "Struct{pin=19,name=MOSI,bcm=10,mode=IN,voltage=0}, " +
                "Struct{pin=20,name=0v}, Struct{pin=21,name=MISO,bcm=9,mode=IN,voltage=0}, " +
                "Struct{pin=22,name=GPIO. 6,mode=IN,voltage=0}, " +
                "Struct{pin=23,name=SCLK,bcm=11,mode=IN,voltage=0}, " +
                "Struct{pin=24,name=CE0,mode=IN,voltage=1}, " +
                "Struct{pin=25,name=0v}, " +
                "Struct{pin=26,name=CE1,mode=IN,voltage=1}, " +
                "Struct{pin=27,name=SDA.0,bcm=0,mode=IN,voltage=1}, " +
                "Struct{pin=28,name=SCL.0,mode=IN,voltage=1}, " +
                "Struct{pin=29,name=GPIO.21,bcm=5,mode=IN,voltage=1}, " +
                "Struct{pin=30,name=0v}, " +
                "Struct{pin=31,name=GPIO.22,bcm=6,mode=IN,voltage=1}, " +
                "Struct{pin=32,name=GPIO.26,mode=IN,voltage=0}, " +
                "Struct{pin=33,name=GPIO.23,bcm=13,mode=IN,voltage=0}, " +
                "Struct{pin=34,name=0v}, " +
                "Struct{pin=35,name=GPIO.24,bcm=19,mode=IN,voltage=0}, " +
                "Struct{pin=36,name=GPIO.27,mode=IN,voltage=0}, " +
                "Struct{pin=37,name=GPIO.25,bcm=26,mode=IN,voltage=0}, " +
                "Struct{pin=38,name=GPIO.28,mode=IN,voltage=0}, " +
                "Struct{pin=39,name=0v}, " +
                "Struct{pin=40,name=GPIO.29,mode=IN,voltage=0}" +
                "]," +
                "createdby=internal," +
                "status=OPERATIONAL,id=xxx-status/b5054ecf-9f18-4b86-bc95-30933fe05581," +
                "operatingsystem=Raspbian GNU/Linux 10 (buster)," +
                "resourcetype=xxx-status," +
                "acl=Struct{" +
                "viewacl=[user/80454ed0-65eb-4b77-864e-2dc525627e38]," +
                "viewmeta=[xxx/d0cee1fb-8fc8-427a-88de-ab4f7b2f8b8b, user/80454ed0-65eb-4b77-864e-2dc525627e38]," +
                "viewdata=[xxx/d0cee1fb-8fc8-427a-88de-ab4f7b2f8b8b, user/80454ed0-65eb-4b77-864e-2dc525627e38]," +
                "editdata=[xxx/d0cee1fb-8fc8-427a-88de-ab4f7b2f8b8b]," +
                "editmeta=[xxx/d0cee1fb-8fc8-427a-88de-ab4f7b2f8b8b]," +
                "owners=[group/nuvla-admin]}," +
                "nextheartbeat=2020-12-11T07:25:15.209Z," +
                "version=1," +
                "resources=Struct{" +
                "cpu=Struct{" +
                "topic=cpu," +
                "rawsample={\"capacity\": 4, \"load\": 0.64}," +
                "capacity=4,load=0.64}," +
                "ram=Struct{" +
                "topic=ram,rawsample={\"capacity\": 3828, \"used\": 1235},capacity=3828,used=1235}," +
                "disks=[" +
                "Struct{" +
                "device=overlay," +
                "capacity=28," +
                "used=4," +
                "topic=disks," +
                "rawsample={\"device\": \"overlay\", \"capacity\": 28, \"used\": 4}" +
                "}" +
                "]," +
                "netstats=[" +
                "Struct{" +
                "interface=docker_gwbridge," +
                "bytestransmitted=1810018," +
                "bytesreceived=633" +
                "}, " +
                "Struct{" +
                "interface=lo," +
                "bytestransmitted=153116745," +
                "bytesreceived=153116745}, " +
                "Struct{interface=veth53b9858,bytestransmitted=3865916,bytesreceived=1275}, " +
                "Struct{interface=vetha95aba6,bytestransmitted=4349209,bytesreceived=0}, " +
                "Struct{interface=docker0,bytestransmitted=58162393,bytesreceived=1347447}, " +
                "Struct{interface=veth2d9e5be,bytestransmitted=20942074,bytesreceived=12350057}, " +
                "Struct{interface=vethe4e283e,bytestransmitted=723871,bytesreceived=352184}, " +
                "Struct{interface=veth5207da0,bytestransmitted=23136462,bytesreceived=61398287}, " +
                "Struct{interface=vethef962b3,bytestransmitted=3858289,bytesreceived=689}, " +
                "Struct{interface=vetha49fdcb,bytestransmitted=3936275,bytesreceived=7957}, " +
                "Struct{interface=br-193effb5470e,bytestransmitted=145658655,bytesreceived=147435494}, " +
                "Struct{interface=wlan0,bytestransmitted=91616660,bytesreceived=307622918}, " +
                "Struct{interface=veth3d6d8ed,bytestransmitted=25273385,bytesreceived=66929714}, " +
                "Struct{interface=eth0,bytestransmitted=0,bytesreceived=0}" +
                "]}," +
                "inferredlocation=[6.0826, 46.1443]," +
                "vulnerabilities=Struct{" +
                "summary=Struct{total=1,affectedproducts=[OpenSSH 7.9p1 Raspbian 10+deb10u2],averagescore=8.1}," +
                "items=[Struct{product=OpenSSH 7.9p1 Raspbian 10+deb10u2,vulnerabilityid=CVE-2019-7639,vulnerabilityscore=8.1}]}" +
                "}", struct.toString());
    }

    @Test
    public void shouldUpcastLongToFloatWhenAtLeastOnEntryIsFloat() {
        Map<String, Object> elasticDocument = mapOf("foo", asList(1, 3.0));

        Schema schema = schemaConverter.convert(elasticDocument, "test");
        Struct struct = structConverter.convert(elasticDocument, schema);

        Assert.assertEquals("Schema{FLOAT64}", schema.field("foo").schema().valueSchema().toString());
        Assert.assertEquals("Struct{foo=[1.0, 3.0]}", struct.toString());
    }

    @Test
    public void shouldUpcastNestedLongToFloatWhenAtLeastOnEntryIsFloat() {
        Map<String, Object> elasticDocument = mapOf("foo", asList(
                mapOf("bar", 1),
                mapOf("bar", 3.0)
        ));

        Schema schema = schemaConverter.convert(elasticDocument, "test");
        Struct struct = structConverter.convert(elasticDocument, schema);

        Assert.assertEquals("Schema{FLOAT64}", schema.field("foo").schema().valueSchema()
                                                     .field("bar").schema().toString());
        Assert.assertEquals("Struct{foo=[Struct{bar=1.0}, Struct{bar=3.0}]}", struct.toString());
    }

    @Test
    public void shouldMergeByMarkingMergedFieldsAsOptional() {
        Map<String, Object> elasticDocument = mapOf("a", asList(emptyMap(), mapOf("b", asList(emptyMap()))));

        Schema schema = schemaConverter.convert(elasticDocument, "test");
        Struct struct = structConverter.convert(elasticDocument, schema);

        Assert.assertEquals("Struct{a=[Struct{}, Struct{b=[Struct{}]}]}", struct.toString());
    }

    @Test
    public void shouldIgnoreFirstListValueIfNull() {
        Map<String, Object> elasticDocument = mapOf("a", asList(null, 3));

        Schema schema = schemaConverter.convert(elasticDocument, "test");
        Struct struct = structConverter.convert(elasticDocument, schema);

        //Assert.assertEquals("Schema{FLOAT64}", schema.toString());
        Assert.assertEquals("Struct{a=[null, 3]}", struct.toString());
    }

    @Test
    public void shouldIgnoreListValuesIfAllNull() {
        Map<String, Object> elasticDocument = mapOf("a", asList(null, null));

        Schema schema = schemaConverter.convert(elasticDocument, "test");
        Struct struct = structConverter.convert(elasticDocument, schema);

        //Assert.assertEquals("Schema{FLOAT64}", schema.toString());
        Assert.assertEquals("Struct{a=[null, null]}", struct.toString());
    }

    private static class NotSupported {
    }
}
