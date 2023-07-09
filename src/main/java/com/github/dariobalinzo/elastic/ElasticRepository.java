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

import com.github.dariobalinzo.elastic.response.Cursor;
import com.github.dariobalinzo.elastic.response.PageResult;
import org.elasticsearch.ElasticsearchStatusException;
import org.elasticsearch.action.search.*;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.elasticsearch.core.TimeValue;
import org.elasticsearch.index.query.BoolQueryBuilder;
import org.elasticsearch.search.builder.PointInTimeBuilder;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.*;
import java.util.stream.Collectors;

import static java.lang.Math.max;
import static org.elasticsearch.index.query.QueryBuilders.*;

public class ElasticRepository {
    private final static Logger logger = LoggerFactory.getLogger(ElasticRepository.class);

    private final ElasticConnection elasticConnection;

    private int pageSize = 5000;
    private final int pitTimeoutSeconds;
    private final int maxPointInTimeTimeoutCount;


    public ElasticRepository(ElasticConnection elasticConnection) {
        this(elasticConnection, 5000, 300, 2);
    }

    public ElasticRepository(ElasticConnection elasticConnection, int pageSize, int pitTimeoutSeconds, int maxPointInTimeTimeoutCount) {
        this.elasticConnection = elasticConnection;
        this.pageSize = max(1, pageSize);
        this.pitTimeoutSeconds = pitTimeoutSeconds = max(35, pitTimeoutSeconds);
        this.maxPointInTimeTimeoutCount = maxPointInTimeTimeoutCount = max(3, maxPointInTimeTimeoutCount);
    }


    protected String openPit(String index) throws IOException {
        Objects.requireNonNull(index, "Index cannot be null");

        OpenPointInTimeRequest openRequest = new OpenPointInTimeRequest(index);
        openRequest.keepAlive(TimeValue.timeValueSeconds(pitTimeoutSeconds));
        OpenPointInTimeResponse openResponse = elasticConnection.getClient().openPointInTime(openRequest, RequestOptions.DEFAULT);
        return openResponse.getPointInTimeId();
    }


    protected void closePit(String pitId) {
        if (pitId == null) {
            return;
        }

        ClosePointInTimeRequest closeRequest = new ClosePointInTimeRequest(pitId);
        try {
            elasticConnection.getClient().closePointInTime(closeRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public PageResult search(Cursor cursor) {
        Objects.requireNonNull(cursor, "Cursor cannot be null");

        final var queryBuilder = cursor.cursorFields().stream()
                .map(cursorField ->
                        boolQuery().must(rangeQuery(cursorField.field()).from(cursorField.initialValue()).includeLower(cursor.includeLower())))
                .reduce(boolQuery(), BoolQueryBuilder::must);

        final PointInTimeBuilder pitBuilder;
        try {
            pitBuilder = new PointInTimeBuilder(Optional.ofNullable(cursor.pitId()).orElse(openPit(cursor.index())));

            // timeout slightly (5s) less than that of the PIT itself
            pitBuilder.setKeepAlive(TimeValue.timeValueSeconds(pitTimeoutSeconds - 5));

            final var searchSourceBuilder = new SearchSourceBuilder()
                    .query(queryBuilder)
                    .pointInTimeBuilder(pitBuilder)
                    .size(pageSize);

            cursor.cursorFields().forEach(cursorField -> searchSourceBuilder.sort(cursorField.field(), SortOrder.ASC));
            Optional.ofNullable(cursor.sortValues()).ifPresent(searchSourceBuilder::searchAfter);

            final var searchRequest = new SearchRequest().source(searchSourceBuilder);
            final var response = executeSearch(searchRequest);

            final var documents = extractDocuments(response);
            final var totalHits = response.getHits().getTotalHits().value;

            final PageResult result;
            if (documents.isEmpty() || totalHits == 0) {
                // return empty page with same cursor to maybe try again later
                result = PageResult.empty(cursor);
            } else if (totalHits > cursor.runningDocumentCount() + documents.size()) {
                // return page with scrollable cursor
                // note: ES says the pitId can change in a query so it's got to be set each query cycle
                final var scrollable = cursor.scrollable(pitBuilder.getEncodedId(),
                        response.getHits().getAt(documents.size() - 1).getSortValues(), documents.size(),
                        totalHits);

                result = PageResult.intermediatePage(documents, scrollable);
            } else {
                // return last page and cursor for the next frame
                final var reframed = cursor.reframe(response.getHits().getAt(documents.size() - 1).getSortValues());
                result = PageResult.lastPage(documents, reframed);
            }

            if (result.lastPage()) {
                closePit(pitBuilder.getEncodedId());
            }

            return result;

        } catch (IOException | InterruptedException e) {
            throw new RuntimeException(e);
        } catch (ElasticsearchStatusException e) {
            // this is to catch when a PIT is timed out and closed by ES before we get to close it, this generally
            // means the PIT timeout is too short for the amount of data being processed and is not usually an issue
            // because the cursor will be re-framed and the PIT re-opened at the new start. It is an issue however
            // if there are too many duplicate sort keys to scroll across in a single PIT timeout.
            if (cursor.isScrollable()) {
                try {
                    closePit(cursor.pitId());
                } catch (RuntimeException ioException) {
                    // nothing, best efforts, probably here because pit was closed unexpectedly anyway (i.e. timed out)
                }

                if (cursor.failureCount() >= maxPointInTimeTimeoutCount) {
                    logger.error("Failed after attempting to scroll cursor {} times, giving up. This is likely caused " +
                                    "by the PIT timeout being too short to scroll through a long list of duplicate " +
                                    "sort keys across multiple poll cycles.",
                            cursor.failureCount(), e);
                }

                // recurse with reframed cursor - if it's not an issue with the pitId it will throw again and this time
                // bubble up because the reframed cursor is not scrollable
                return search(cursor.reframe(cursor.sortValues(), true, true));
            }

            // if not scrollable it's likely something else re-throw
            throw e;
        }
    }

    private List<Map<String, Object>> extractDocuments(SearchResponse response) {
        return Arrays.stream(response.getHits().getHits())
                .map(hit -> {
                    Map<String, Object> sourceMap = hit.getSourceAsMap();
                    sourceMap.put("es-id", hit.getId());
                    sourceMap.put("es-index", hit.getIndex());
                    return sourceMap;
                }).collect(Collectors.toList());
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

    public List<String> catIndices(String prefix) {
        Response resp;
        try {

            resp = elasticConnection.getClient()
                    .getLowLevelClient()
                    .performRequest(new Request("GET", "/_cat/indices"));
        } catch (IOException e) {
            logger.error("error in searching index names");
            throw new RuntimeException(e);
        }

        List<String> result = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new InputStreamReader(resp.getEntity().getContent()))) {
            String line;

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
                    .performRequest(new Request("POST", "/" + index + "/_refresh"));
        } catch (IOException e) {
            logger.error("error in refreshing index " + index);
            throw new RuntimeException(e);
        }
    }

    public void setPageSize(int pageSize) {
        this.pageSize = pageSize;
    }
}
