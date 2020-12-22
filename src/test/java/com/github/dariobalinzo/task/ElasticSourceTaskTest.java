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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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

        //when fetching (second page)
        List<SourceRecord> poll2 = task.poll();
        assertEquals(2, poll2.size());

        //then
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

        //when fetching (second page)
        List<SourceRecord> poll2 = task.poll();
        assertEquals(1, poll2.size());

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
        assertEquals("Struct{fullName=Test}", poll1.get(0).value().toString());

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
        assertEquals("Struct{fullName=\"Test\",nonavrofield=non-avro-field,avroField=avro-field,age=10,ts=111}", poll1.get(0).value().toString());

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
        assertEquals("Struct{fullName=Test,nonavrofield=non-avro-field,avroField=avro-field,age=10,ts=111}", poll1.get(0).value().toString());

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
        conf.put(ElasticSourceConnectorConfig.CONNECTOR_FIELDNAME_CONVERTER_CONFIG, ElasticSourceConnectorConfig.NOP_FIELDNAME_CONVERTER);

        //when (fetching first page)
        task.start(conf);
        List<SourceRecord> poll1 = task.poll();
        assertEquals("Struct{fullName=Test,non-avro-field=non-avro-field,avroField=avro-field,age=10,ts=111}", poll1.get(0).value().toString());

        task.stop();
    }



}
