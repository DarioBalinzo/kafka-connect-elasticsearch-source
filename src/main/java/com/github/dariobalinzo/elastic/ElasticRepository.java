package com.github.dariobalinzo.elastic;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;

public final class ElasticRepository {
    private final static Logger logger = LoggerFactory.getLogger(ElasticRepository.class);

    private final ElasticConnection elasticConnection;
    private final String cursorField;

    private int pageSize = 5000;

    public ElasticRepository(ElasticConnection elasticConnection, String cursorField) {
        this.elasticConnection = elasticConnection;
        this.cursorField = cursorField;
    }

    public ElasticRepository(ElasticConnection elasticConnection) {
        this.elasticConnection = elasticConnection;
        this.cursorField = "_id";
    }

    public PageResult searchAfter(String index, String cursor) throws IOException, InterruptedException {
        QueryBuilder queryBuilder = cursor == null ?
                matchAllQuery() :
                rangeQuery(cursorField).from(cursor, false);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(queryBuilder)
                .size(pageSize)
                .sort(cursorField, SortOrder.ASC);

        SearchRequest searchRequest = new SearchRequest(index)
                .source(searchSourceBuilder);

        SearchResponse response = executeSearch(searchRequest);

        List<Map<String, Object>> documents = Arrays.stream(response.getHits().getHits())
                .map(SearchHit::getSourceAsMap)
                .collect(Collectors.toList());
        return new PageResult(index, documents, cursorField);
    }

    private SearchResponse executeSearch(SearchRequest searchRequest) throws IOException, InterruptedException {
        int maxTrials = elasticConnection.getMaxConnectionAttempts();
        if (maxTrials <= 0) {
            throw new IllegalArgumentException("MaxConnectionAttempts should be > 0");
        }
        IOException lastError = null;
        for (int i = 0; i < maxTrials; ++i) {
            try {
                return elasticConnection.getClient()
                        .search(searchRequest);
            } catch (IOException e) {
                lastError = e;
                Thread.sleep(elasticConnection.getConnectionRetryBackoff());
            }
        }
        throw lastError;
    }

    public List<String> catIndices(String prefix) {
        Response resp;
        try {
            resp = elasticConnection.getClient()
                    .getLowLevelClient()
                    .performRequest("GET", "_cat/indices");
        } catch (IOException e) {
            logger.error("error in searching index names");
            throw new RuntimeException(e);
        }

        List<String> result = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(resp.getEntity().getContent()))) {
            String line = null;

            while ((line = reader.readLine()) != null) {
                String index = line.split("\\s+")[2];
                if (index.startsWith(prefix)) {
                    result.add(index);
                }
            }
        } catch (IOException e) {
            logger.error("error while getting indices", e);
        }

        return result;
    }

    public void refreshIndex(String index) {
        try {
            elasticConnection.getClient()
                    .getLowLevelClient()
                    .performRequest("POST", "/"+ index +"/_refresh");
        } catch (IOException e) {
            logger.error("error in refreshing index " + index);
            throw new RuntimeException(e);
        }
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }
}
