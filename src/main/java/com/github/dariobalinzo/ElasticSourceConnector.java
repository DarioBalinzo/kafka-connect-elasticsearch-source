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

package com.github.dariobalinzo;

import com.github.dariobalinzo.elastic.ElasticConnection;
import com.github.dariobalinzo.elastic.ElasticConnectionBuilder;
import com.github.dariobalinzo.elastic.ElasticRepository;
import com.github.dariobalinzo.elastic.ElasticIndexMonitorThread;
import com.github.dariobalinzo.task.ElasticSourceTask;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

public class ElasticSourceConnector extends SourceConnector {
    private static Logger logger = LoggerFactory.getLogger(ElasticSourceConnector.class);
    private static final long MAX_TIMEOUT = 10000L;
    private static final long POLL_MILISSECONDS = 5000L;

    private ElasticSourceConnectorConfig config;
    private ElasticConnection elasticConnection;
    private ElasticRepository elasticRepository;
    private Map<String, String> configProperties;
    private ElasticIndexMonitorThread indexMonitorThread;

    @Override
    public String version() {
        return Version.VERSION;
    }

    @Override
    public void start(Map<String, String> props) {
        try {
            configProperties = props;
            config = new ElasticSourceConnectorConfig(props);
        } catch (ConfigException e) {
            throw new ConnectException("Couldn't start ElasticSourceConnector due to configuration "
                    + "error", e);
        }

        String esScheme = config.getString(ElasticSourceConnectorConfig.ES_SCHEME_CONF);
        String esHost = config.getString(ElasticSourceConnectorConfig.ES_HOST_CONF);

        //using rest config all the parameters are strings
        int esPort = Integer.parseInt(config.getString(ElasticSourceConnectorConfig.ES_PORT_CONF));

        String esUser = config.getString(ElasticSourceConnectorConfig.ES_USER_CONF);
        String esPwd = config.getString(ElasticSourceConnectorConfig.ES_PWD_CONF);

        int maxConnectionAttempts = Integer.parseInt(config.getString(
                ElasticSourceConnectorConfig.CONNECTION_ATTEMPTS_CONFIG
        ));
        long connectionRetryBackoff = Long.parseLong(config.getString(
                ElasticSourceConnectorConfig.CONNECTION_BACKOFF_CONFIG
        ));

        ElasticConnectionBuilder connectionBuilder = new ElasticConnectionBuilder(esHost, esPort)
                .withProtocol(esScheme)
                .withMaxAttempts(maxConnectionAttempts)
                .withBackoff(connectionRetryBackoff);

        String truststore = config.getString(ElasticSourceConnectorConfig.ES_TRUSTSTORE_CONF);
        String truststorePass = config.getString(ElasticSourceConnectorConfig.ES_TRUSTSTORE_PWD_CONF);
        String keystore = config.getString(ElasticSourceConnectorConfig.ES_KEYSTORE_CONF);
        String keystorePass = config.getString(ElasticSourceConnectorConfig.ES_KEYSTORE_PWD_CONF);

        if (truststore != null) {
            connectionBuilder.withTrustStore(truststore, truststorePass);
        }

        if (keystore != null) {
            connectionBuilder.withKeyStore(keystore, keystorePass);
        }

        if (esUser == null || esUser.isEmpty()) {
            elasticConnection = connectionBuilder.build();
        } else {
            elasticConnection = connectionBuilder.withUser(esUser)
                    .withPassword(esPwd)
                    .build();
        }

        elasticRepository = new ElasticRepository(elasticConnection);

        indexMonitorThread = new ElasticIndexMonitorThread(context, POLL_MILISSECONDS, elasticRepository, config.getString(ElasticSourceConnectorConfig.INDEX_PREFIX_CONFIG));
        indexMonitorThread.start();
    }

    @Override
    public Class<? extends Task> taskClass() {
        return ElasticSourceTask.class;
    }


    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {
        if (configProperties.containsKey(ElasticSourceConnectorConfig.INDEX_NAMES_CONFIG)) {
            String indicesNames = configProperties.get(ElasticSourceConnectorConfig.INDEX_NAMES_CONFIG);
            String[] indicesList = indicesNames.split(",");
            return generateTaskFromFixedList(Arrays.asList(indicesList), maxTasks);
        } else {
            return findTaskFromIndexPrefix(maxTasks);
        }
    }

    private List<Map<String, String>> generateTaskFromFixedList(List<String> indicesList, int maxTasks) {
        int numGroups = Math.min(indicesList.size(), maxTasks);
        return groupIndicesToTasksConfig(maxTasks, indicesList);
    }

    private List<Map<String, String>> findTaskFromIndexPrefix(int maxTasks) {
        List<String> currentIndexes = indexMonitorThread.indexes();
        return groupIndicesToTasksConfig(maxTasks, currentIndexes);
    }

    private List<Map<String, String>> groupIndicesToTasksConfig(int maxTasks, List<String> currentIndexes) {
        int numGroups = Math.min(currentIndexes.size(), maxTasks);
        List<List<String>> indexGrouped = groupPartitions(currentIndexes, numGroups);
        List<Map<String, String>> taskConfigs = new ArrayList<>(indexGrouped.size());
        for (List<String> taskIndices : indexGrouped) {
            Map<String, String> taskProps = new HashMap<>(configProperties);
            taskProps.put(ElasticSourceConnectorConfig.INDICES_CONFIG,
                    String.join(",", taskIndices));
            taskConfigs.add(taskProps);
        }
        return taskConfigs;
    }

    @Override
    public void stop() {
        logger.info("stopping elastic source");
        indexMonitorThread.shutdown();
        try {
            indexMonitorThread.join(MAX_TIMEOUT);
        } catch (InterruptedException e) {
        // Ignore, shouldn't be interrupted
        }
        elasticConnection.closeQuietly();
    }

    @Override
    public ConfigDef config() {
        return ElasticSourceConnectorConfig.CONFIG_DEF;
    }


    private List<List<String>> groupPartitions(List<String> currentIndices, int numGroups) {
        List<List<String>> result = new ArrayList<>(numGroups);
        for (int i = 0; i < numGroups; ++i) {
            result.add(new ArrayList<>());
        }

        for (int i = 0; i < currentIndices.size(); ++i) {
            result.get(i % numGroups).add(currentIndices.get(i));
        }

        return result;
    }
}
