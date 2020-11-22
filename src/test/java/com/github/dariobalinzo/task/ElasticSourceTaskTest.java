package com.github.dariobalinzo.task;

import com.github.dariobalinzo.ElasticSourceConnectorConfig;
import com.github.dariobalinzo.elastic.ElasticConnection;
import com.github.dariobalinzo.elastic.ElasticRepository;
import com.github.dariobalinzo.elastic.PageResult;
import org.apache.http.HttpHost;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTaskContext;
import org.apache.kafka.connect.storage.OffsetStorageReader;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.runners.MockitoJUnitRunner;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import java.io.IOException;
import java.util.*;

import static org.junit.Assert.*;

public class ElasticSourceTaskTest {
    private static final Logger logger = LoggerFactory.getLogger(ElasticSourceTaskTest.class);

    private static final int TEST_PAGE_SIZE = 3;
    private static final int MAX_TRIALS = 2;
    private static final int RETRY_WAIT_MS = 1_000;

    private static final String TEST_INDEX = "source_index";
    private static final String CURSOR_FIELD = "ts";

    private static final String ELASTICSEARCH_IMAGE = "docker.elastic.co/elasticsearch/elasticsearch:7.9.3";

    private static ElasticsearchContainer container;
    private static ElasticConnection connection;
    private static ElasticRepository repository;

    @Mock
    private SourceTaskContext context;

    @BeforeClass
    public static void setupElastic() {
        // Create the elasticsearch container.
        container = new ElasticsearchContainer(ELASTICSEARCH_IMAGE);
        container.start();

        HttpHost httpHost = HttpHost.create(container.getHttpHostAddress());
        connection = new ElasticConnection(
                httpHost.getHostName(),
                httpHost.getPort(),
                MAX_TRIALS,
                RETRY_WAIT_MS
        );

        repository = new ElasticRepository(connection, CURSOR_FIELD);
        repository.setPageSize(TEST_PAGE_SIZE);
    }

    @Before
    public void init() {
        MockitoAnnotations.initMocks(this);

        OffsetStorageReader emptyOffset = new OffsetStorageReader() {
            @Override
            public <T> Map<String, Object> offset(Map<String, T> map) {
                return new HashMap<>();
            }

            @Override
            public <T> Map<Map<String, T>, Map<String, Object>> offsets(Collection<Map<String, T>> collection) {
                return new HashMap<>();
            }
        };

        Mockito.when(context.offsetStorageReader()).thenReturn(emptyOffset);
    }



    @Test
    public void shouldRunSourceTask() throws IOException, InterruptedException {
        //given
        deleteTestIndex();

        insertMockData(111);
        insertMockData(112);
        insertMockData(113);
        insertMockData(114);
        refreshIndex();

        ElasticSourceTask task = new ElasticSourceTask();
        HttpHost httpHost = HttpHost.create(container.getHttpHostAddress());

        Map<String, String> conf = new HashMap<>();
        conf.put(ElasticSourceTaskConfig.INDICES_CONFIG, TEST_INDEX);
        conf.put(ElasticSourceConnectorConfig.TOPIC_PREFIX_CONFIG, "topic");
        conf.put(ElasticSourceConnectorConfig.INCREMENTING_FIELD_NAME_CONFIG, CURSOR_FIELD);
        conf.put(ElasticSourceConnectorConfig.POLL_INTERVAL_MS_CONFIG, String.valueOf(10));
        conf.put(ElasticSourceConnectorConfig.ES_HOST_CONF, httpHost.getHostName());
        conf.put(ElasticSourceConnectorConfig.ES_PORT_CONF, String.valueOf(httpHost.getPort()));
        conf.put(ElasticSourceConnectorConfig.BATCH_MAX_ROWS_CONFIG, String.valueOf(2));
        conf.put(ElasticSourceConnectorConfig.CONNECTION_ATTEMPTS_CONFIG, String.valueOf(MAX_TRIALS));
        conf.put(ElasticSourceConnectorConfig.CONNECTION_BACKOFF_CONFIG, String.valueOf(RETRY_WAIT_MS));

        task.initialize(context);

        //when (fetching first page)
        task.start(conf);
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

    private void deleteTestIndex() {
        try {
            connection.getClient().indices().delete(new DeleteIndexRequest(TEST_INDEX));
        } catch (Exception ignored) {

        }
    }

    private void refreshIndex() throws IOException, InterruptedException {
        //connection.getClient().indices().refresh(new RefreshRequest(TEST_INDEX), RequestOptions.DEFAULT);
        Thread.sleep(5_000);
    }

    private void insertMockData(int tsStart) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .field("fullName", "Test")
                .field(CURSOR_FIELD, tsStart)
                .field("age", 10)
                .endObject();

        IndexRequest indexRequest = new IndexRequest(TEST_INDEX);
        indexRequest.type("_doc");
        indexRequest.source(builder);

        IndexResponse response = connection.getClient().index(indexRequest);
        assertEquals(DocWriteResponse.Result.CREATED, response.getResult());
    }


    @AfterClass
    public static void stopElastic() {
        if (container != null) {
            container.close();
        }
    }

}
