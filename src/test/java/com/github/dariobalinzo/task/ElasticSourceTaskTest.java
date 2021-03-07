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

package com.github.dariobalinzo.task;

import com.github.dariobalinzo.ElasticSourceConnectorConfig;
import com.github.dariobalinzo.TestContainersContext;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import static com.github.dariobalinzo.ElasticSourceConnectorConfig.SECONDARY_INCREMENTING_FIELD_NAME_CONFIG;
import static org.junit.Assert.*;

public class ElasticSourceTaskTest extends TestContainersContext {

    @Mock
    private SourceTaskContext context;

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void shouldRunSourceTaskWithoutInitialOffset() throws IOException, InterruptedException {
        //given
        deleteTestIndex();

        insertMockData(111);
        insertMockData(112);
        insertMockData(113);
        insertMockData(114);
        refreshIndex();

        ElasticSourceTask task = new ElasticSourceTask();
        Mockito.when(context.offsetStorageReader()).thenReturn(MockOffsetFactory.empty());
        task.initialize(context);

        //when (fetching first page)
        task.start(getConf());
        List<SourceRecord> poll1 = task.poll();
        assertEquals(2, poll1.size());
        assertEquals(111L,
                ((Struct) poll1.get(0).value()).get("ts")
        );
        assertEquals("{position=111}", poll1.get(0).sourceOffset().toString());
        assertEquals(
                112L,
                ((Struct) poll1.get(1).value()).get("ts")
        );
        assertEquals("{position=112}", poll1.get(1).sourceOffset().toString());

        //when fetching (second page)
        List<SourceRecord> poll2 = task.poll();
        assertEquals(2, poll2.size());
        assertEquals(
                113L,
                ((Struct) poll2.get(0).value()).get("ts")
        );
        assertEquals("{position=113}", poll2.get(0).sourceOffset().toString());
        assertEquals(
                114L,
                ((Struct) poll2.get(1).value()).get("ts")
        );
        assertEquals("{position=114}", poll2.get(1).sourceOffset().toString());

        //then
        List<SourceRecord> empty = task.poll();
        assertTrue(empty.isEmpty());

        task.stop();
    }

    @Test
    public void shouldRunTask_WithSecondarySort_WithoutInitialOffset() throws IOException, InterruptedException {
        //given
        deleteTestIndex();

        insertMockData(111, "customerA", TEST_INDEX);
        insertMockData(111, "customerB", TEST_INDEX);
        insertMockData(111, "customerC", TEST_INDEX);
        insertMockData(111, "customerD", TEST_INDEX);
        insertMockData(112, "customerA", TEST_INDEX);
        refreshIndex();

        ElasticSourceTask task = new ElasticSourceTask();
        Mockito.when(context.offsetStorageReader()).thenReturn(MockOffsetFactory.empty());
        task.initialize(context);

        //when (fetching first page)
        Map<String, String> conf = getConf();
        conf.put(SECONDARY_INCREMENTING_FIELD_NAME_CONFIG, SECONDARY_CURSOR_FIELD);
        task.start(conf);
        List<SourceRecord> poll1 = task.poll();
        assertEquals(2, poll1.size());
        assertEquals(
                "customerA",
                ((Struct) poll1.get(0).value()).get("fullName")
        );
        assertEquals(
                111L,
                ((Struct) poll1.get(0).value()).get("ts")
        );
        assertEquals("{position_secondary=customerA, position=111}", poll1.get(0).sourceOffset().toString());
        assertEquals(
                "customerB",
                ((Struct) poll1.get(1).value()).get("fullName")
        );
        assertEquals(
                111L,
                ((Struct) poll1.get(1).value()).get("ts")
        );
        assertEquals("{position_secondary=customerB, position=111}", poll1.get(1).sourceOffset().toString());

        //when fetching (second page)
        List<SourceRecord> poll2 = task.poll();
        assertEquals(2, poll2.size());
        assertEquals(
                "customerC",
                ((Struct) poll2.get(0).value()).get("fullName")
        );
        assertEquals(
                111L,
                ((Struct) poll2.get(0).value()).get("ts")
        );
        assertEquals("{position_secondary=customerC, position=111}", poll2.get(0).sourceOffset().toString());
        assertEquals(
                "customerD",
                ((Struct) poll2.get(1).value()).get("fullName")
        );
        assertEquals(
                111L,
                ((Struct) poll2.get(1).value()).get("ts")
        );
        assertEquals("{position_secondary=customerD, position=111}", poll2.get(1).sourceOffset().toString());

        //then
        List<SourceRecord> last = task.poll();
        assertEquals(1, last.size());
        assertEquals(
                "customerA",
                ((Struct) last.get(0).value()).get("fullName")
        );
        assertEquals(
                112L,
                ((Struct) last.get(0).value()).get("ts")
        );
        assertEquals("{position_secondary=customerA, position=112}", last.get(0).sourceOffset().toString());

        List<SourceRecord> empty = task.poll();
        assertTrue(empty.isEmpty());

        task.stop();
    }

    @Test
    public void shouldRunSourceTaskWithInitialOffset() throws IOException, InterruptedException {
        //given
        deleteTestIndex();

        insertMockData(111);
        insertMockData(112);
        insertMockData(113);
        insertMockData(114);
        refreshIndex();

        ElasticSourceTask task = new ElasticSourceTask();
        Mockito.when(context.offsetStorageReader()).thenReturn(MockOffsetFactory.from(String.valueOf(111)));
        task.initialize(context);

        //when (fetching first page)
        task.start(getConf());
        List<SourceRecord> poll1 = task.poll();

        assertEquals(2, poll1.size());

        assertEquals(
                "Test",
                ((Struct) poll1.get(0).value()).get("fullName")
        );
        assertEquals(
                112L,
                ((Struct) poll1.get(0).value()).get("ts")
        );
        assertEquals("{position=112}", poll1.get(0).sourceOffset().toString());
        assertEquals(
                "Test",
                ((Struct) poll1.get(1).value()).get("fullName")
        );
        assertEquals(
                113L,
                ((Struct) poll1.get(1).value()).get("ts")
        );
        assertEquals("{position=113}", poll1.get(1).sourceOffset().toString());

        //when fetching (second page)
        List<SourceRecord> poll2 = task.poll();
        assertEquals(1, poll2.size());
        assertEquals(
                "Test",
                ((Struct) poll2.get(0).value()).get("fullName")
        );
        assertEquals(
                114L,
                ((Struct) poll2.get(0).value()).get("ts")
        );
        assertEquals("{position=114}", poll2.get(0).sourceOffset().toString());

        //then
        List<SourceRecord> empty = task.poll();
        assertTrue(empty.isEmpty());

        task.stop();
    }

    @Test
    public void shouldRunTask_WithSecondarySort_WithOnlyPrimaryInitialOffset() throws IOException, InterruptedException {
        //given
        deleteTestIndex();

        insertMockData(110, "customerA", TEST_INDEX); //already seen...
        insertMockData(111, "customerA", TEST_INDEX);
        insertMockData(111, "customerB", TEST_INDEX);
        insertMockData(111, "customerC", TEST_INDEX);
        insertMockData(111, "customerD", TEST_INDEX);
        insertMockData(112, "customerA", TEST_INDEX);
        refreshIndex();

        ElasticSourceTask task = new ElasticSourceTask();
        Mockito.when(context.offsetStorageReader()).thenReturn(MockOffsetFactory.from(String.valueOf(110)));
        task.initialize(context);

        //when (fetching first page)
        Map<String, String> conf = getConf();
        conf.put(SECONDARY_INCREMENTING_FIELD_NAME_CONFIG, SECONDARY_CURSOR_FIELD);
        task.start(conf);
        List<SourceRecord> poll1 = task.poll();
        assertEquals(2, poll1.size());
        assertEquals(
                "customerA",
                ((Struct) poll1.get(0).value()).get("fullName")
        );
        assertEquals(
                111L,
                ((Struct) poll1.get(0).value()).get("ts")
        );
        assertEquals(
                "customerB",
                ((Struct) poll1.get(1).value()).get("fullName")
        );
        assertEquals(
                111L,
                ((Struct) poll1.get(1).value()).get("ts")
        );

        //when fetching (second page)
        List<SourceRecord> poll2 = task.poll();
        assertEquals(
                "customerC",
                ((Struct) poll2.get(0).value()).get("fullName")
        );
        assertEquals(
                111L,
                ((Struct) poll2.get(0).value()).get("ts")
        );
        assertEquals("{position_secondary=customerC, position=111}", poll2.get(0).sourceOffset().toString());
        assertEquals(
                "customerD",
                ((Struct) poll2.get(1).value()).get("fullName")
        );
        assertEquals(
                111L,
                ((Struct) poll2.get(1).value()).get("ts")
        );
        assertEquals("{position_secondary=customerD, position=111}", poll2.get(1).sourceOffset().toString());
        assertEquals(2, poll2.size());

        //then
        List<SourceRecord> last = task.poll();
        assertEquals(1, last.size());
        assertEquals(
                "customerA",
                ((Struct) last.get(0).value()).get("fullName")
        );
        assertEquals(
                112L,
                ((Struct) last.get(0).value()).get("ts")
        );
        assertEquals("{position_secondary=customerA, position=112}", last.get(0).sourceOffset().toString());
        List<SourceRecord> empty = task.poll();
        assertTrue(empty.isEmpty());

        task.stop();
    }

    @Test
    public void shouldRunTask_WithSecondarySort_WithInitialOffset() throws IOException, InterruptedException {
        //given
        deleteTestIndex();

        insertMockData(110, "customerA", TEST_INDEX); //already seen
        insertMockData(111, "customerA", TEST_INDEX); //already seen
        insertMockData(111, "customerB", TEST_INDEX);
        insertMockData(111, "customerC", TEST_INDEX);
        insertMockData(111, "customerD", TEST_INDEX);
        insertMockData(112, "customerA", TEST_INDEX);
        refreshIndex();

        ElasticSourceTask task = new ElasticSourceTask();
        Mockito.when(context.offsetStorageReader()).thenReturn(MockOffsetFactory.from(String.valueOf(111), "customerA"));
        task.initialize(context);

        //when (fetching first page)
        Map<String, String> conf = getConf();
        conf.put(SECONDARY_INCREMENTING_FIELD_NAME_CONFIG, SECONDARY_CURSOR_FIELD);
        task.start(conf);
        List<SourceRecord> poll1 = task.poll();
        assertEquals(2, poll1.size());
        assertEquals(
                "customerB",
                ((Struct) poll1.get(0).value()).get("fullName")
        );
        assertEquals("{position_secondary=customerB, position=111}", poll1.get(0).sourceOffset().toString());
        assertEquals(
                "customerC",
                ((Struct) poll1.get(1).value()).get("fullName")
        );

        //when fetching (second page)
        List<SourceRecord> poll2 = task.poll();
        assertEquals(2, poll2.size());
        assertEquals(
                "customerD",
                ((Struct) poll2.get(0).value()).get("fullName")
        );
        assertEquals("{position_secondary=customerD, position=111}", poll2.get(0).sourceOffset().toString());
        assertEquals(
                "customerA",
                ((Struct) poll2.get(1).value()).get("fullName")
        );
        assertEquals("{position_secondary=customerA, position=112}", poll2.get(1).sourceOffset().toString());

        //then
        List<SourceRecord> empty = task.poll();
        assertTrue(empty.isEmpty());

        task.stop();
    }


    @Test
    public void shouldRunSourceTaskWhitelist() throws IOException, InterruptedException {
        //given
        deleteTestIndex();

        insertMockData(111);
        insertMockData(112);
        insertMockData(113);
        insertMockData(114);
        refreshIndex();

        ElasticSourceTask task = new ElasticSourceTask();
        Mockito.when(context.offsetStorageReader()).thenReturn(MockOffsetFactory.empty());
        task.initialize(context);
        Map<String, String> conf = getConf();
        conf.put(ElasticSourceConnectorConfig.FIELDS_WHITELIST_CONFIG, "fullName");

        //when (fetching first page)
        task.start(conf);
        List<SourceRecord> poll1 = task.poll();
        //Check the struct contains one only field, and this is "FullName" = "Test"
        assertEquals(1, ((Struct) poll1.get(0).value()).schema().fields().size());
        assertEquals(((Struct) poll1.get(0).value()).get("fullName"), "Test");
        task.stop();
    }

    @Test
    public void shouldRunSourceTaskBlacklist() throws IOException, InterruptedException {
        //given
        deleteTestIndex();

        insertMockData(111);
        insertMockData(112);
        insertMockData(113);
        insertMockData(114);
        refreshIndex();

        ElasticSourceTask task = new ElasticSourceTask();
        Mockito.when(context.offsetStorageReader()).thenReturn(MockOffsetFactory.empty());
        task.initialize(context);
        Map<String, String> conf = getConf();
        conf.put(ElasticSourceConnectorConfig.FIELDS_BLACKLIST_CONFIG, "fullName");

        //when (fetching first page)
        task.start(conf);
        List<SourceRecord> poll1 = task.poll();

        //Then
        List<Field> fields = ((Struct) poll1.get(0).value()).schema().fields();
        assertEquals(6, fields.size());
        task.stop();
    }

    @Test
    public void shouldRunSourceTaskWithJsonCastFilter() throws IOException, InterruptedException {
        //given
        deleteTestIndex();

        insertMockData(111);
        insertMockData(112);
        insertMockData(113);
        insertMockData(114);
        refreshIndex();

        ElasticSourceTask task = new ElasticSourceTask();
        Mockito.when(context.offsetStorageReader()).thenReturn(MockOffsetFactory.empty());
        task.initialize(context);
        Map<String, String> conf = getConf();
        conf.put(ElasticSourceConnectorConfig.FIELDS_JSON_CAST_CONFIG, "fullName");

        //when (fetching first page)
        task.start(conf);
        List<SourceRecord> poll1 = task.poll();
        Struct structValue = (Struct) poll1.get(0).value();
        assertEquals(structValue.get("fullName"), "\"Test\"");
        assertEquals(structValue.get("avroField"), "avro-field");
        assertEquals(structValue.get("nonavrofield"), "non-avro-field");
        task.stop();
    }

    @Test
    public void shouldRunSourceTaskWithAvroNameConverter() throws IOException, InterruptedException {
        //given
        deleteTestIndex();

        insertMockData(111);
        insertMockData(112);
        insertMockData(113);
        insertMockData(114);
        refreshIndex();

        ElasticSourceTask task = new ElasticSourceTask();
        Mockito.when(context.offsetStorageReader()).thenReturn(MockOffsetFactory.empty());
        task.initialize(context);
        Map<String, String> conf = getConf();

        //when (fetching first page)
        task.start(conf);
        List<SourceRecord> poll1 = task.poll();
        Struct structValue = (Struct) poll1.get(0).value();
        assertEquals(structValue.get("avroField"), "avro-field");
        assertEquals(structValue.get("nonavrofield"), "non-avro-field");
        task.stop();
    }

    @Test
    public void shouldRunSourceTaskWithNopNameConverter() throws IOException, InterruptedException {
        //given
        deleteTestIndex();

        insertMockData(111);
        insertMockData(112);
        insertMockData(113);
        insertMockData(114);
        refreshIndex();

        ElasticSourceTask task = new ElasticSourceTask();
        Mockito.when(context.offsetStorageReader()).thenReturn(MockOffsetFactory.empty());
        task.initialize(context);
        Map<String, String> conf = getConf();
        conf.put(ElasticSourceConnectorConfig.CONNECTOR_FIELDNAME_CONVERTER_CONFIG,
                ElasticSourceConnectorConfig.NOP_FIELDNAME_CONVERTER);

        //when (fetching first page)
        task.start(conf);
        List<SourceRecord> poll1 = task.poll();
        Struct structValue = (Struct) poll1.get(0).value();
        assertEquals(structValue.get("avroField"), "avro-field");
        assertEquals(structValue.get("non-avro-field"), "non-avro-field");
        task.stop();
    }


}
