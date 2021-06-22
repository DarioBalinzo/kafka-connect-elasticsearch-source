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

public class ElasticConnectionBuilder {
    final String hosts;
    final int port;

    String protocol = "http";
    int maxConnectionAttempts = 3;
    long connectionRetryBackoff = 1_000;
    String user;
    String pwd;

    String trustStorePath;
    String trustStorePassword;
    String keyStorePath;
    String keyStorePassword;

    public ElasticConnectionBuilder(String hosts, int port) {
        this.hosts = hosts;
        this.port = port;
    }

    public ElasticConnectionBuilder withProtocol(String protocol) {
        this.protocol = protocol;
        return this;
    }

    public ElasticConnectionBuilder withUser(String user) {
        this.user = user;
        return this;
    }

    public ElasticConnectionBuilder withPassword(String password) {
        this.pwd = password;
        return this;
    }

    public ElasticConnectionBuilder withMaxAttempts(int maxConnectionAttempts) {
        this.maxConnectionAttempts = maxConnectionAttempts;
        return this;
    }

    public ElasticConnectionBuilder withBackoff(long connectionRetryBackoff) {
        this.connectionRetryBackoff = connectionRetryBackoff;
        return this;
    }

    public ElasticConnectionBuilder withTrustStore(String path, String password) {
        this.trustStorePath = path;
        this.trustStorePassword = password;
        return this;
    }

    public ElasticConnectionBuilder withKeyStore(String path, String password) {
        this.keyStorePath = path;
        this.keyStorePassword = password;
        return this;
    }

    public ElasticConnection build() {
        return new ElasticConnection(this);
    }

}
