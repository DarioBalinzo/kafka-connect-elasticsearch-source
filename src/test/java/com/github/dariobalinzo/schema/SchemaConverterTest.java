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
import java.util.*;

public class SchemaConverterTest {

    private final SchemaConverter schemaConverter = new SchemaConverter(new AvroName());
    private final StructConverter structConverter = new StructConverter(new AvroName());
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
                "Struct{mode=IN,pin=3,name=SDA.1,bcm=2,voltage=1}, " +
                "Struct{pin=4,name=5v}, " +
                "Struct{mode=IN,pin=5,name=SCL.1,bcm=3,voltage=1}, " +
                "Struct{pin=6,name=0v}, Struct{mode=IN,pin=7,name=GPIO. 7,bcm=4,voltage=1}, " +
                "Struct{mode=IN,pin=8,name=TxD,voltage=1}, Struct{pin=9,name=0v}, " +
                "Struct{mode=IN,pin=10,name=RxD,voltage=1}, Struct{mode=IN,pin=11,name=GPIO. 0,bcm=17,voltage=0}, " +
                "Struct{mode=IN,pin=12,name=GPIO. 1,voltage=0}, Struct{mode=IN,pin=13,name=GPIO. 2,bcm=27,voltage=0}, " +
                "Struct{pin=14,name=0v}, Struct{mode=IN,pin=15,name=GPIO. 3,bcm=22,voltage=0}, " +
                "Struct{mode=IN,pin=16,name=GPIO. 4,voltage=0}, " +
                "Struct{pin=17,name=3.3v}, " +
                "Struct{mode=IN,pin=18,name=GPIO. 5,voltage=0}, " +
                "Struct{mode=IN,pin=19,name=MOSI,bcm=10,voltage=0}, " +
                "Struct{pin=20,name=0v}, Struct{mode=IN,pin=21,name=MISO,bcm=9,voltage=0}, " +
                "Struct{mode=IN,pin=22,name=GPIO. 6,voltage=0}, " +
                "Struct{mode=IN,pin=23,name=SCLK,bcm=11,voltage=0}, " +
                "Struct{mode=IN,pin=24,name=CE0,voltage=1}, " +
                "Struct{pin=25,name=0v}, " +
                "Struct{mode=IN,pin=26,name=CE1,voltage=1}, " +
                "Struct{mode=IN,pin=27,name=SDA.0,bcm=0,voltage=1}, " +
                "Struct{mode=IN,pin=28,name=SCL.0,voltage=1}, " +
                "Struct{mode=IN,pin=29,name=GPIO.21,bcm=5,voltage=1}, " +
                "Struct{pin=30,name=0v}, " +
                "Struct{mode=IN,pin=31,name=GPIO.22,bcm=6,voltage=1}, " +
                "Struct{mode=IN,pin=32,name=GPIO.26,voltage=0}, " +
                "Struct{mode=IN,pin=33,name=GPIO.23,bcm=13,voltage=0}, " +
                "Struct{pin=34,name=0v}, " +
                "Struct{mode=IN,pin=35,name=GPIO.24,bcm=19,voltage=0}, " +
                "Struct{mode=IN,pin=36,name=GPIO.27,voltage=0}, " +
                "Struct{mode=IN,pin=37,name=GPIO.25,bcm=26,voltage=0}, " +
                "Struct{mode=IN,pin=38,name=GPIO.28,voltage=0}, " +
                "Struct{pin=39,name=0v}, " +
                "Struct{mode=IN,pin=40,name=GPIO.29,voltage=0}" +
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
                "topic=disks," +
                "rawsample={\"device\": \"overlay\", \"capacity\": 28, \"used\": 4}," +
                "used=4," +
                "device=overlay," +
                "capacity=28" +
                "}" +
                "]," +
                "netstats=[" +
                "Struct{" +
                "bytestransmitted=1810018," +
                "bytesreceived=633," +
                "interface=docker_gwbridge" +
                "}, " +
                "Struct{" +
                "bytestransmitted=153116745," +
                "bytesreceived=153116745," +
                "interface=lo}, " +
                "Struct{bytestransmitted=3865916,bytesreceived=1275,interface=veth53b9858}, " +
                "Struct{bytestransmitted=4349209,bytesreceived=0,interface=vetha95aba6}, " +
                "Struct{bytestransmitted=58162393,bytesreceived=1347447,interface=docker0}, " +
                "Struct{bytestransmitted=20942074,bytesreceived=12350057,interface=veth2d9e5be}, " +
                "Struct{bytestransmitted=723871,bytesreceived=352184,interface=vethe4e283e}, " +
                "Struct{bytestransmitted=23136462,bytesreceived=61398287,interface=veth5207da0}, " +
                "Struct{bytestransmitted=3858289,bytesreceived=689,interface=vethef962b3}, " +
                "Struct{bytestransmitted=3936275,bytesreceived=7957,interface=vetha49fdcb}, " +
                "Struct{bytestransmitted=145658655,bytesreceived=147435494,interface=br-193effb5470e}, " +
                "Struct{bytestransmitted=91616660,bytesreceived=307622918,interface=wlan0}, " +
                "Struct{bytestransmitted=25273385,bytesreceived=66929714,interface=veth3d6d8ed}, " +
                "Struct{bytestransmitted=0,bytesreceived=0,interface=eth0}" +
                "]}," +
                "inferredlocation=[6.0826, 46.1443]," +
                "vulnerabilities=Struct{" +
                "summary=Struct{total=1,affectedproducts=[OpenSSH 7.9p1 Raspbian 10+deb10u2],averagescore=8.1}," +
                "items=[Struct{product=OpenSSH 7.9p1 Raspbian 10+deb10u2,vulnerabilityid=CVE-2019-7639,vulnerabilityscore=8.1}]}" +
                "}", struct.toString());
    }

    private static class NotSupported {
    }
}
