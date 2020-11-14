package com.github.dariobalinzo.elastic;

import org.apache.http.HttpHost;
import org.elasticsearch.action.DocWriteResponse;
import org.elasticsearch.action.admin.indices.delete.DeleteIndexRequest;
import org.elasticsearch.action.admin.indices.refresh.RefreshRequest;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.common.xcontent.XContentFactory;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ElasticRepositoryTest {
    private static final Logger logger = LoggerFactory.getLogger(ElasticRepositoryTest.class);

    private static final int TEST_PAGE_SIZE = 3;
    private static final int MAX_TRIALS = 2;
    private static final int RETRY_WAIT_MS = 1_000;

    private static final String TEST_INDEX = "source_index";
    private static final String CURSOR_FIELD = "ts";

    private static final String ELASTICSEARCH_IMAGE = "docker.elastic.co/elasticsearch/elasticsearch:7.9.3";

    private static ElasticsearchContainer container;
    private static ElasticConnection connection;
    private static ElasticRepository repository;

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


    @Test
    public void shouldFetchDataFromElastic() throws IOException, InterruptedException {
        deleteTestIndex();

        insertMockData(111);
        insertMockData(112);
        insertMockData(113);
        insertMockData(114);
        refreshIndex();

        PageResult firstPage = repository.searchAfter(TEST_INDEX, null);
        assertEquals(3, firstPage.getDocuments().size());

        PageResult secondPage = repository.searchAfter(TEST_INDEX, firstPage.getLastCursor());
        assertEquals(1, secondPage.getDocuments().size());

        PageResult emptyPage = repository.searchAfter(TEST_INDEX, secondPage.getLastCursor());
        assertEquals(0, emptyPage.getDocuments().size());
        assertNull(emptyPage.getLastCursor());

        assertEquals(Collections.singletonList(TEST_INDEX), repository.catIndices("source"));
        assertEquals(Collections.emptyList(), repository.catIndices("non-existing"));
    }

    private void deleteTestIndex() {
        try {
            connection.getClient().indices().delete(new DeleteIndexRequest(TEST_INDEX), RequestOptions.DEFAULT);
        } catch (Exception ignored) {

        }
    }

    private void refreshIndex() throws IOException {
        connection.getClient().indices().refresh(new RefreshRequest(TEST_INDEX), RequestOptions.DEFAULT);
    }

    private void insertMockData(int tsStart) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder()
                .startObject()
                .field("fullName", "Test")
                .field(CURSOR_FIELD, tsStart)
                .field("age", 10)
                .endObject();

        IndexRequest indexRequest = new IndexRequest(TEST_INDEX);
        indexRequest.source(builder);

        IndexResponse response = connection.getClient().index(indexRequest, RequestOptions.DEFAULT);
        assertEquals(DocWriteResponse.Result.CREATED, response.getResult());
    }


    @AfterClass
    public static void stopElastic() {
        if (container != null) {
            container.close();
        }
    }

}
