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
import org.apache.http.ssl.SSLContextBuilder;
import org.apache.http.ssl.SSLContexts;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.net.ssl.SSLContext;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.security.KeyStore;
import java.util.Arrays;
import java.util.Objects;

public class ElasticConnection {
    public final static Logger logger = LoggerFactory.getLogger(ElasticConnection.class);

    private RestHighLevelClient client;
    private final long connectionRetryBackoff;
    private final int maxConnectionAttempts;
    private final String hosts;
    private final String protocol;
    private final int port;
    private final SSLContext sslContext;
    private final CredentialsProvider credentialsProvider;

    ElasticConnection(ElasticConnectionBuilder builder) {
        hosts = builder.hosts;
        protocol = builder.protocol;
        port = builder.port;

        String user = builder.user;
        String pwd = builder.pwd;
        if (user != null) {
            credentialsProvider = new BasicCredentialsProvider();
            credentialsProvider.setCredentials(AuthScope.ANY,
                    new UsernamePasswordCredentials(user, pwd));
        } else {
            credentialsProvider = null;
        }

        sslContext = builder.trustStorePath == null ? null :
                getSslContext(
                    builder.trustStorePath,
                    builder.trustStorePassword,
                    builder.keyStorePath,
                    builder.keyStorePassword
                );

        createConnection();

        this.maxConnectionAttempts = builder.maxConnectionAttempts;
        this.connectionRetryBackoff = builder.connectionRetryBackoff;
    }

    private void createConnection() {
        HttpHost[] hostList = parseHosts(hosts, protocol, port);

        client = new RestHighLevelClient(
                RestClient.builder(hostList)
                        .setHttpClientConfigCallback(
                                httpClientBuilder -> {
                                    if (credentialsProvider != null) {
                                        httpClientBuilder.setDefaultCredentialsProvider(credentialsProvider);
                                    }
                                    if (sslContext != null) {
                                        httpClientBuilder.setSSLContext(sslContext);
                                    }
                                    return httpClientBuilder;
                                }
                        )
        );
    }

    private SSLContext getSslContext(String trustStoreConf, String trustStorePass,
                             String keyStoreConf, String keyStorePass) {

        Objects.requireNonNull(trustStoreConf, "truststore location is required");
        Objects.requireNonNull(trustStorePass, "truststore password is required");

        try {
            Path trustStorePath = Paths.get(trustStoreConf);
            KeyStore truststore = KeyStore.getInstance("pkcs12");
            try (InputStream is = Files.newInputStream(trustStorePath)) {
                truststore.load(is, trustStorePass.toCharArray());
            }
            SSLContextBuilder sslBuilder = SSLContexts.custom()
                    .loadTrustMaterial(truststore, null);

            if (keyStoreConf != null) {
                Objects.requireNonNull(keyStorePass, "keystore password is required");
                Path keyStorePath = Paths.get(keyStoreConf);
                KeyStore keyStore = KeyStore.getInstance("pkcs12");
                try (InputStream is = Files.newInputStream(keyStorePath)) {
                    keyStore.load(is, keyStorePass.toCharArray());
                }
                sslBuilder.loadKeyMaterial(keyStore, keyStorePass.toCharArray());
            }

            return sslBuilder.build();
        } catch (Exception e) {
            throw new SslContextException(e);
        }
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
