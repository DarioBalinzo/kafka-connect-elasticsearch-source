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

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class ElasticConnection {
    public final static Logger logger = LoggerFactory.getLogger(ElasticConnection.class);

    final RestHighLevelClient client;
    private final long connectionRetryBackoff;
    private final int maxConnectionAttempts;

    public ElasticConnection(String host, int port, int maxConnectionAttempts,
                             long connectionRetryBackoff) {
        logger.info("elastic auth disabled");

        //TODO add configuration for https also, and many nodes instead of only one
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(host, port)));

        this.maxConnectionAttempts = maxConnectionAttempts;
        this.connectionRetryBackoff = connectionRetryBackoff;

    }

    public ElasticConnection(String host, int port, String user, String pwd,
                             int maxConnectionAttempts, long connectionRetryBackoff) {

        logger.info("elastic auth enabled");

        final CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(user, pwd));

        //TODO add configuration for https also, and many nodes instead of only one
        client = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost(host, port)).setHttpClientConfigCallback(
                        httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                )
        );

        this.maxConnectionAttempts = maxConnectionAttempts;
        this.connectionRetryBackoff = connectionRetryBackoff;

    }

    public RestHighLevelClient getClient() {
        return client;
    }

    public long getConnectionRetryBackoff() {
        return connectionRetryBackoff;
    }

    public int getMaxConnectionAttempts() {
        return maxConnectionAttempts;
    }

    public void closeQuietly() {
        try {
            client.close();
        } catch (IOException e) {
            logger.error("error in close", e);
        }
    }

}
