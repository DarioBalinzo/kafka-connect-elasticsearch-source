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

import com.github.dariobalinzo.task.ElasticSourceTask;
import com.github.dariobalinzo.utils.ElasticConnection;
import com.github.dariobalinzo.utils.Utils;
import com.github.dariobalinzo.utils.Version;
import com.sun.deploy.util.StringUtils;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.common.config.types.Password;
import org.apache.kafka.connect.connector.Task;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceConnector;
import org.elasticsearch.client.Response;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ElasticSourceConnector extends SourceConnector {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSourceConnector.class);
    private ElasticSourceConnectorConfig config;
    private ElasticConnection elasticConnection;
    private Map<String, String> configProperties;

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
            elasticConnection = new ElasticConnection(
                    esHost,
                    esPort,
                    maxConnectionAttempts,
                    connectionRetryBackoff
            );
        } else {
            elasticConnection = new ElasticConnection(
                    esHost,
                    esPort,
                    esUser,
                    esPwd.value(),
                    maxConnectionAttempts,
                    connectionRetryBackoff
            );

        }

        // Initial connection attempt
        if (!elasticConnection.testConnection()) {
            throw new ConfigException("cannot connect to es");
        }

    }

    @Override
    public Class<? extends Task> taskClass() {
        return ElasticSourceTask.class;
    }


    @Override
    public List<Map<String, String>> taskConfigs(int maxTasks) {

        Response resp;
        try {
            resp = elasticConnection.getClient().getLowLevelClient().performRequest("GET", "_cat/indices");
        } catch (IOException e) {
            logger.error("error in searching index names");
            throw new RuntimeException(e);
        }


        List<String> currentIndexes = Utils.getIndexList(resp, config.getString(ElasticSourceConnectorConfig.INDEX_PREFIX_CONFIG));
        int numGroups = Math.min(currentIndexes.size(), maxTasks);
        List<List<String>> tablesGrouped = Utils.groupPartitions(currentIndexes, numGroups);
        List<Map<String, String>> taskConfigs = new ArrayList<>(tablesGrouped.size());
        for (List<String> taskTables : tablesGrouped) {
            Map<String, String> taskProps = new HashMap<>(configProperties);
            taskProps.put(ElasticSourceConnectorConfig.INDICES_CONFIG,
                    StringUtils.join(taskTables, ","));
            taskConfigs.add(taskProps);
        }
        return taskConfigs;
    }

    @Override
    public void stop() {

        logger.info("stopping elastic source");
        elasticConnection.closeQuietly();

    }

    @Override
    public ConfigDef config() {
        return ElasticSourceConnectorConfig.CONFIG_DEF;
    }
}
