package com.github.dariobalinzo;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.atLeast;

import java.io.IOException;

import com.github.dariobalinzo.elastic.ElasticIndexMonitorThread;

import org.apache.kafka.connect.connector.ConnectorContext;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;


public class ElasticIndexMonitorThreadTest extends TestContainersContext {

    @Mock
    private ConnectorContext context;

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void shouldRefreshIndexesList() throws InterruptedException, IOException {
        //given
        long pollInterval = 1000L;
        deleteTestIndex();

        insertMockData(10, TEST_INDEX);
        refreshIndex();

        ElasticIndexMonitorThread indexMonitorThread = new ElasticIndexMonitorThread(context, pollInterval, repository, TEST_INDEX);
        indexMonitorThread.start();

        assertEquals(1, indexMonitorThread.indexes().size());

        //when another index is created in Elastic
        insertMockData(10, TEST_INDEX + '2');
        refreshIndex();

        long waitRefresh = pollInterval + (long)(Math.random() * 1000);
        Thread.sleep(waitRefresh);

        //then
        Mockito.verify(context, atLeast(1)).requestTaskReconfiguration();
        assertEquals(2, indexMonitorThread.indexes().size());

        indexMonitorThread.shutdown();
    }
}
