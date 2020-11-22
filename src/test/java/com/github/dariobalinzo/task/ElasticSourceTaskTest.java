package com.github.dariobalinzo.task;

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

}
