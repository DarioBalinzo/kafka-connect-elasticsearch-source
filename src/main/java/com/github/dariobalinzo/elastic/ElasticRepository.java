package com.github.dariobalinzo.elastic;

import com.github.dariobalinzo.utils.ElasticConnection;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static org.elasticsearch.index.query.QueryBuilders.matchAllQuery;
import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;

public final class ElasticRepository {
    private final ElasticConnection elasticConnection;
    private final String index;
    private final String cursorField;

    private int pageSize = 5000;

    public ElasticRepository(ElasticConnection elasticConnection, String index, String cursorField) {
        this.elasticConnection = elasticConnection;
        this.index = index;
        this.cursorField = cursorField;
    }

    public PageResult searchAfter(String cursor) throws IOException, InterruptedException {
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
        return new PageResult(documents, cursorField);
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
                        .search(searchRequest, RequestOptions.DEFAULT);
            } catch (IOException e) {
                lastError = e;
                Thread.sleep(elasticConnection.getConnectionRetryBackoff());
            }
        }
        throw lastError;
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }
}
