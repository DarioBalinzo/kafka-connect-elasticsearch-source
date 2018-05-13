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

import com.github.dariobalinzo.ElasticSourceConnector;
import com.github.dariobalinzo.ElasticSourceConnectorConfig;
import com.github.dariobalinzo.utils.Version;
import com.github.dariobalinzo.utils.ElasticConnection;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.FieldSortBuilder;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import static org.elasticsearch.index.query.QueryBuilders.termQuery;

public class ElasticSourceTask extends SourceTask {

    private static final Logger log = LoggerFactory.getLogger(ElasticSourceTask.class);

    private ElasticSourceTaskConfig config;
    private ElasticConnection es;

    private AtomicBoolean stopping;

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

        List<String> indices = config.getList(ElasticSourceTaskConfig.INDICES_CONFIG);
        if ( indices.isEmpty() ) {
            throw new ConnectException("Invalid configuration: each ElasticSourceTask must have at "
                    + "least one index assigned to it");
        }
    }

    private void initEsConnection() {

        final String esHost = config.getString(ElasticSourceConnectorConfig.ES_HOST_CONF);
        final int esPort = config.getInt(ElasticSourceConnectorConfig.ES_PORT_CONF);

        final String esUser = config.getString(ElasticSourceConnectorConfig.ES_USER_CONF);
        final Password esPwd = config.getPassword(ElasticSourceConnectorConfig.ES_PWD_CONF);

        final int maxConnectionAttempts = config.getInt(
                ElasticSourceConnectorConfig.CONNECTION_ATTEMPTS_CONFIG
        );
        final long connectionRetryBackoff = config.getLong(
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

    public List<SourceRecord> poll() throws InterruptedException {

        List<SourceRecord> result = new ArrayList<>();
        QueryBuilder qb = termQuery("multi", "test");

//        SearchResponse scrollResp = client
//                .addSort(FieldSortBuilder.DOC_FIELD_NAME, SortOrder.ASC)
//                .setScroll(new TimeValue(60000))
//                .setQuery(qb)
//                .setSize(100).get(); //max of 100 hits will be returned for each scroll
//Scroll until no hits are returned
//        do {
//            for (SearchHit hit : scrollResp.getHits().getHits()) {
//                //Handle the hit...
//                SourceRecord record = new SourceRecord();
//            }

            //scrollResp = client.prepareSearchScroll(scrollResp.getScrollId()).setScroll(new TimeValue(60000)).execute().actionGet();
        //} while(scrollResp.getHits().getHits().length != 0); // Zero hits mark the end of the scroll and the while loop.


        return result;
    }

    public void stop() {

    }

}
