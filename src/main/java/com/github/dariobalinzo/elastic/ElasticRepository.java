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

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.json.jackson.JacksonJsonpMapper;
import co.elastic.clients.transport.ElasticsearchTransport;
import co.elastic.clients.transport.rest_client.RestClientTransport;
import com.github.dariobalinzo.elastic.response.Cursor;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;

import static java.lang.Math.max;

public class ElasticRepository {
    private final static Logger logger = LoggerFactory.getLogger(ElasticRepository.class);

    private final ElasticConnection elasticConnection;

    private int pageSize = 5000;
    private final int pitTimeoutSeconds;


    public ElasticRepository(ElasticConnection elasticConnection) {
        this(elasticConnection, 5000, 300);
    }

    public ElasticRepository(ElasticConnection elasticConnection, int pageSize, int pitTimeoutSeconds) {
        this.elasticConnection = elasticConnection;
        this.pageSize = max(1, pageSize);
        this.pitTimeoutSeconds = max(35, pitTimeoutSeconds);
    }

    protected ElasticConnection getElasticConnection() {
        return elasticConnection;
    }

    public ESResultIterator getIterator(Cursor cursor) {
        ElasticsearchTransport transport = new RestClientTransport(
            this.elasticConnection.getClient().getLowLevelClient(),
            new JacksonJsonpMapper()
        );

        // uses the newer java api client
        return new ESResultIterator(new ElasticsearchClient(transport), cursor, pageSize, pitTimeoutSeconds);
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
