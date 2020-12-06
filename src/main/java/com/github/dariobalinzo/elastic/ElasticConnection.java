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
import java.util.Arrays;

public class ElasticConnection {
    public final static Logger logger = LoggerFactory.getLogger(ElasticConnection.class);

    private RestHighLevelClient client;
    private final long connectionRetryBackoff;
    private final int maxConnectionAttempts;

    ElasticConnection(ElasticConnectionBuilder builder) {
        String hosts = builder.hosts;
        String user = builder.user;
        String pwd = builder.pwd;
        String protocol = builder.protocol;
        int port = builder.port;

        if (user == null) {
            createConnectionWithNoAuth(hosts, protocol, port);
        } else {
            createConnectionUsingAuth(hosts, protocol, port, user, pwd);
        }

        this.maxConnectionAttempts = builder.maxConnectionAttempts;
        this.connectionRetryBackoff = builder.connectionRetryBackoff;
    }

    private void createConnectionWithNoAuth(String hosts, String protocol, int port) {
        logger.info("elastic auth disabled");
        HttpHost[] hostList = parseHosts(hosts, protocol, port);
        client = new RestHighLevelClient(RestClient.builder(hostList));
    }

    private void createConnectionUsingAuth(String hosts, String protocol, int port, String user, String pwd) {
        logger.info("elastic auth enabled");
        CredentialsProvider credentialsProvider = new BasicCredentialsProvider();
        credentialsProvider.setCredentials(AuthScope.ANY,
                new UsernamePasswordCredentials(user, pwd));


        HttpHost[] hostList = parseHosts(hosts, protocol, port);

        client = new RestHighLevelClient(
                RestClient.builder(hostList)
                        .setHttpClientConfigCallback(
                                httpClientBuilder -> httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider)
                        )
        );
    }

    private HttpHost[] parseHosts(String hosts, String protocol, int port) {
        return Arrays.stream(hosts.split(";"))
                .map(host -> new HttpHost(host, port, protocol))
                .toArray(HttpHost[]::new);
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
