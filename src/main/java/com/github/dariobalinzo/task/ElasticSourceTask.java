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

package com.github.dariobalinzo.task;

import com.github.dariobalinzo.ElasticSourceConnectorConfig;
import com.github.dariobalinzo.schema.SchemaConverter;
import com.github.dariobalinzo.utils.ElasticConnection;
import com.github.dariobalinzo.utils.Version;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.elasticsearch.action.search.*;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.search.Scroll;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.index.query.QueryBuilders.rangeQuery;

public class ElasticSourceTask extends SourceTask {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSourceTask.class);

    private ElasticSourceTaskConfig config;
    private ElasticConnection es;

    private AtomicBoolean stopping = new AtomicBoolean(false);
    private List<String> indices;
    private long connectionRetryBackoff;
    private int maxConnectionAttempts;

    public ElasticSourceTask() {

    }

    @Override
    public String version() {
        return Version.VERSION;
    }

    @Override
    public void start(Map<String, String> properties) {
        try {
            config = new ElasticSourceTaskConfig(properties);
        } catch (ConfigException e) {
            throw new ConnectException("Couldn't start ElasticSourceTask due to configuration error", e);
        }

        initEsConnection();

        indices = config.getList(ElasticSourceTaskConfig.INDICES_CONFIG);
        if (indices.isEmpty()) {
            throw new ConnectException("Invalid configuration: each ElasticSourceTask must have at "
                    + "least one index assigned to it");
        }
    }

    private void initEsConnection() {

        final String esHost = config.getString(ElasticSourceConnectorConfig.ES_HOST_CONF);
        final int esPort = config.getInt(ElasticSourceConnectorConfig.ES_PORT_CONF);

        final String esUser = config.getString(ElasticSourceConnectorConfig.ES_USER_CONF);
        final Password esPwd = config.getPassword(ElasticSourceConnectorConfig.ES_PWD_CONF);

        maxConnectionAttempts = config.getInt(
                ElasticSourceConnectorConfig.CONNECTION_ATTEMPTS_CONFIG
        );
        connectionRetryBackoff = config.getLong(
                ElasticSourceConnectorConfig.CONNECTION_BACKOFF_CONFIG
        );
        if (esUser == null || esUser.isEmpty()) {
            es = new ElasticConnection(
                    esHost,
                    esPort,
                    maxConnectionAttempts,
                    connectionRetryBackoff
            );
        } else {
            es = new ElasticConnection(
                    esHost,
                    esPort,
                    esUser,
                    esPwd.value(),
                    maxConnectionAttempts,
                    connectionRetryBackoff
            );

        }

    }

    //will be called by connect with a different thread than the stop thread
    public List<SourceRecord> poll() throws InterruptedException {

        List<SourceRecord> results = new ArrayList<>();
        indices.forEach(
                index -> {
                    if (!stopping.get()) {
                        logger.info("fetching from {}",index);
                        String lastValue = fetchLastOffset();
                        logger.info("found last value {}",lastValue);
                        if (lastValue != null) {
                            executeScroll(index, lastValue, results);
                        }
                    }
                }
        );
        return results;
    }

    private String fetchLastOffset() {
        return null;
    }

    private void executeScroll(String index, String lastValue, List<SourceRecord> results) {
        final Scroll scroll = new Scroll(TimeValue.timeValueMinutes(1L));
        SearchRequest searchRequest = new SearchRequest(index);
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(
                rangeQuery(config.getString(ElasticSourceConnectorConfig.INCREMENTING_FIELD_NAME_CONFIG))
                        .from(lastValue)
        ); //TODO configure custom query
        searchSourceBuilder.size(config.getInt(ElasticSourceConnectorConfig.BATCH_MAX_ROWS_CONFIG));
        searchRequest.source(searchSourceBuilder);
        SearchResponse searchResponse = null;
        String scrollId = null;
        try {
            for (int i = 0; i < maxConnectionAttempts; ++i) {
                try {
                    searchResponse = es.getClient().search(searchRequest);
                    break;
                } catch (IOException e) {
                    logger.error("error in scroll");
                    Thread.sleep(connectionRetryBackoff);
                }
            }
            scrollId = searchResponse.getScrollId();
            SearchHits hits = searchResponse.getHits();
            int totalShards = searchResponse.getTotalShards();
            int successfulShards = searchResponse.getSuccessfulShards();

            logger.info("total shard {}, successuful: {}", totalShards, successfulShards);
            logger.info("retrived {}, scroll id : {}", hits, scrollId);

            int failedShards = searchResponse.getFailedShards();
            for (ShardSearchFailure failure : searchResponse.getShardFailures()) {
                // failures should be handled here
                logger.error("failed {}", failure);
            }
            if (failedShards > 0) {
                throw new RuntimeException("failed shard in search");
            }

            SearchHit[] searchHits = hits.getHits();
            for (SearchHit hit : searchHits) {
                // do something with the SearchHit
                Map sourcePartition = Collections.singletonMap("index", index);
                Map sourceOffset = Collections.singletonMap("position", lastValue);
                Map<String, Object> sourceAsMap = hit.getSourceAsMap();
                SourceRecord sourceRecord = new SourceRecord(
                        sourcePartition,
                        sourceOffset,
                        "",
                        SchemaConverter.convertElasticMapping2AvroSchema(sourceAsMap,index),
""

                );
            }

            while (searchHits != null && searchHits.length > 0) {
                SearchScrollRequest scrollRequest = new SearchScrollRequest(scrollId);
                scrollRequest.scroll(scroll);
                searchResponse = es.getClient().searchScroll(scrollRequest);
                scrollId = searchResponse.getScrollId();
                searchHits = searchResponse.getHits().getHits();

            }
        } catch (Throwable t) {
            logger.error("error",t);
        } finally {
            closeScrollQuietly(scrollId);
        }


    }

    private void closeScrollQuietly(String scrollId)  {
        ClearScrollRequest clearScrollRequest = new ClearScrollRequest();
        clearScrollRequest.addScrollId(scrollId);
        ClearScrollResponse clearScrollResponse = null;
        try {
            clearScrollResponse = es.getClient().clearScroll(clearScrollRequest);
        } catch (IOException e) {
            logger.error("error in clear scroll",e);
        }
        boolean succeeded = clearScrollResponse.isSucceeded();
        logger.info("scroll {} cleared: {}",scrollId,succeeded);
    }

    //will be called by connect with a different thread than poll thread
    public void stop() {

        if (stopping != null) {
            stopping.set(true);
        }

        if (es != null) {
            es.closeQuietly();
        }

    }

}
