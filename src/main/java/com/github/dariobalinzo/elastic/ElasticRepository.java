/**
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

package com.github.dariobalinzo.elastic;

import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Response;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.stream.Collectors;

import static org.elasticsearch.index.query.QueryBuilders.*;

public final class ElasticRepository {
    private final static Logger logger = LoggerFactory.getLogger(ElasticRepository.class);

    private final ElasticConnection elasticConnection;
    private final String cursorField;
    private final String secondaryCursorField;

    private int pageSize = 5000;

    public ElasticRepository(ElasticConnection elasticConnection) {
        this.elasticConnection = elasticConnection;
        this.cursorField = "_id";
        this.secondaryCursorField = null;
    }

    public ElasticRepository(ElasticConnection elasticConnection, String cursorField) {
        this.elasticConnection = elasticConnection;
        this.cursorField = cursorField;
        this.secondaryCursorField = null;
    }

    public ElasticRepository(ElasticConnection elasticConnection, String cursorField, String secondaryCursorField) {
        this.elasticConnection = elasticConnection;
        this.cursorField = cursorField;
        this.secondaryCursorField = secondaryCursorField;
    }

    public PageResult searchAfter(String index, String cursor) throws IOException, InterruptedException {
        QueryBuilder queryBuilder = cursor == null ?
                matchAllQuery() :
                rangeQuery(cursorField).from(cursor, false);

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(queryBuilder)
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

    public PageResult searchAfter(String index, String primaryCursor, String secondaryCursor) throws IOException, InterruptedException {
        Objects.requireNonNull(secondaryCursorField);
        Objects.requireNonNull(primaryCursor);
        Objects.requireNonNull(secondaryCursor);

        BoolQueryBuilder queryBuilder = boolQuery()
                .minimumShouldMatch(1)
                .should(rangeQuery(cursorField).from(primaryCursor, false))
                .should(
                        boolQuery()
                                .filter(matchQuery(cursorField, primaryCursor))
                                .filter(rangeQuery(secondaryCursorField).from(secondaryCursor, false))
                );

        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder()
                .query(queryBuilder)
                .size(pageSize)
                .sort(cursorField, SortOrder.ASC)
                .sort(secondaryCursorField, SortOrder.ASC);

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
                    .performRequest("GET", "/_cat/indices");
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
                    .performRequest("POST", "/" + index + "/_refresh");
        } catch (IOException e) {
            logger.error("error in refreshing index " + index);
            throw new RuntimeException(e);
        }
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }
}
