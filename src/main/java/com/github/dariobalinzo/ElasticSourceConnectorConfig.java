/*
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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;

import java.util.Collections;
import java.util.Map;

public class ElasticSourceConnectorConfig extends AbstractConfig {

    //TODO add the possibility to specify multiple hosts
    public final static String ES_HOST_CONF = "es.host";
    private final static String ES_HOST_DOC = "ElasticSearch host";
    private final static String ES_HOST_DISPLAY = "Elastic host";

    public final static String ES_PORT_CONF = "es.port";
    private final static String ES_PORT_DOC = "ElasticSearch port";
    private final static String ES_PORT_DISPLAY = "ElasticSearch port";

    public final static String ES_QUERY = "es.query";
    private final static String ES_QUERY_DOC = "ElasticSearch string query";
    private final static String ES_QUERY_DISPLAY = "ElasticSearch string query";

    public final static String ES_USER_CONF = "es.user";
    private final static String ES_USER_DOC = "Elasticsearch username";
    private final static String ES_USER_DISPLAY = "Elasticsearch username";

    public final static String ES_PWD_CONF = "es.password";
    private final static String ES_PWD_DOC = "Elasticsearch password";
    private final static String ES_PWD_DISPLAY = "Elasticsearch password";

    public static final String CONNECTION_ATTEMPTS_CONFIG = "connection.attempts";
    private static final String CONNECTION_ATTEMPTS_DOC
            = "Maximum number of attempts to retrieve a valid Elasticsearch connection.";
    private static final String CONNECTION_ATTEMPTS_DISPLAY = "Elasticsearch connection attempts";
    private static final String CONNECTION_ATTEMPTS_DEFAULT = "3";

    public static final String CONNECTION_BACKOFF_CONFIG = "connection.backoff.ms";
    private static final String CONNECTION_BACKOFF_DOC
            = "Backoff time in milliseconds between connection attempts.";
    private static final String CONNECTION_BACKOFF_DISPLAY
            = "Elastic connection backoff in milliseconds";
    private static final String CONNECTION_BACKOFF_DEFAULT = "10000";

    public static final String POLL_INTERVAL_MS_CONFIG = "poll.interval.ms";
    private static final String POLL_INTERVAL_MS_DOC = "Frequency in ms to poll for new data in "
            + "each index.";
    private static final String POLL_INTERVAL_MS_DEFAULT = "5000";
    private static final String POLL_INTERVAL_MS_DISPLAY = "Poll Interval (ms)";

    public static final String BATCH_MAX_ROWS_CONFIG = "batch.max.rows";
    private static final String BATCH_MAX_ROWS_DOC =
            "Maximum number of documents to include in a single batch when polling for new data.";
    private static final String BATCH_MAX_ROWS_DEFAULT = "10000";
    private static final String BATCH_MAX_ROWS_DISPLAY = "Max Documents Per Batch";

    private static final String MODE_UNSPECIFIED = "";
    private static final String MODE_BULK = "bulk";
    private static final String MODE_TIMESTAMP = "timestamp";
    private static final String MODE_INCREMENTING = "incrementing";
    private static final String MODE_TIMESTAMP_INCREMENTING = "timestamp+incrementing";

    public static final String INCREMENTING_FIELD_NAME_CONFIG = "incrementing.field.name";
    private static final String INCREMENTING_FIELD_NAME_DOC =
            "The name of the strictly incrementing field to use to detect new records.";
    private static final String INCREMENTING_FIELD_NAME_DEFAULT = "";
    private static final String INCREMENTING_FIELD_NAME_DISPLAY = "Incrementing Field Name";

    public static final String INDEX_PREFIX_CONFIG = "index.prefix";
    private static final String INDEX_PREFIX_DOC = "List of indices to include in copying.";
    private static final String INDEX_PREFIX_DEFAULT = "";
    private static final String INDEX_PREFIX_DISPLAY = "Indices prefix Whitelist";


    public static final String TOPIC_CONFIG = "topic";
    private static final String TOPIC_DOC = "The Kafka topic to publish data";
    private static final String TOPIC_DISPLAY = "Kafka topic";

    public static final String LABEL_KEY = "label.key";
    private static final String LABEL_KEY_DOC = "The key of the label to add to each record";
    private static final String LABEL_KEY_DISPLAY = "Label key";

    public static final String LABEL_VALUE = "label.value";
    private static final String LABEL_VALUE_DOC = "The value of the label to add to each record";
    private static final String LABEL_VALUE_DISPLAY = "Label value";

    private static final String DATABASE_GROUP = "Elasticsearch";
    private static final String MODE_GROUP = "Mode";
    private static final String CONNECTOR_GROUP = "Connector";
    private static final String LABELING_GROUP = "Labeling";

    private static final String MODE_CONFIG = "mode";
    private static final String MODE_DOC = "";
    private static final String MODE_DISPLAY = "Index Incrementing field";

    public static final String INDICES_CONFIG = "es.indices";


    public static final ConfigDef CONFIG_DEF = baseConfigDef();


    protected static ConfigDef baseConfigDef() {
        ConfigDef config = new ConfigDef();
        addDatabaseOptions(config);
        addModeOptions(config);
        addConnectorOptions(config);
        addLabelOptions(config);
        return config;
    }

    private static void addDatabaseOptions(ConfigDef config) {
        int orderInGroup = 0;
        config.define(
                ES_HOST_CONF,
                Type.STRING,
                Importance.HIGH,
                ES_HOST_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.LONG,
                ES_HOST_DISPLAY,
                Collections.singletonList(INDEX_PREFIX_CONFIG)
        ).define(
                ES_PORT_CONF,
                Type.STRING,
                Importance.HIGH,
                ES_PORT_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.LONG,
                ES_PORT_DISPLAY,
                Collections.singletonList(INDEX_PREFIX_CONFIG)
        ).define(
                ES_QUERY,
                Type.STRING,
                Importance.HIGH,
                ES_QUERY_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.LONG,
                ES_QUERY_DISPLAY
        ).define(
                ES_USER_CONF,
                Type.STRING,
                null,
                Importance.HIGH,
                ES_USER_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.LONG,
                ES_USER_DISPLAY
        ).define(
                ES_PWD_CONF,
                Type.STRING,
                null,
                Importance.HIGH,
                ES_PWD_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.SHORT,
                ES_PWD_DISPLAY
        ).define(
                CONNECTION_ATTEMPTS_CONFIG,
                Type.STRING,
                CONNECTION_ATTEMPTS_DEFAULT,
                Importance.LOW,
                CONNECTION_ATTEMPTS_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                ConfigDef.Width.SHORT,
                CONNECTION_ATTEMPTS_DISPLAY
        ).define(
                CONNECTION_BACKOFF_CONFIG,
                Type.STRING,
                CONNECTION_BACKOFF_DEFAULT,
                Importance.LOW,
                CONNECTION_BACKOFF_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.SHORT,
                CONNECTION_BACKOFF_DISPLAY
        ).define(
                INDEX_PREFIX_CONFIG,
                Type.STRING,
                INDEX_PREFIX_DEFAULT,
                Importance.MEDIUM,
                INDEX_PREFIX_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.LONG,
                INDEX_PREFIX_DISPLAY
        );
    }

    private static void addModeOptions(ConfigDef config) {
        int orderInGroup = 0;
        config.define(
                MODE_CONFIG,
                Type.STRING,
                MODE_UNSPECIFIED,
                ConfigDef.ValidString.in(
                        MODE_UNSPECIFIED,
                        MODE_BULK,
                        MODE_TIMESTAMP,
                        MODE_INCREMENTING,
                        MODE_TIMESTAMP_INCREMENTING
                ),
                Importance.HIGH,
                MODE_DOC,
                MODE_GROUP,
                ++orderInGroup,
                Width.MEDIUM,
                MODE_DISPLAY,
                Collections.singletonList(
                        INCREMENTING_FIELD_NAME_CONFIG
                )
        ).define(
                INCREMENTING_FIELD_NAME_CONFIG,
                Type.STRING,
                INCREMENTING_FIELD_NAME_DEFAULT,
                Importance.MEDIUM,
                INCREMENTING_FIELD_NAME_DOC,
                MODE_GROUP,
                ++orderInGroup,
                Width.MEDIUM,
                INCREMENTING_FIELD_NAME_DISPLAY
        );
    }

    private static void addConnectorOptions(ConfigDef config) {
        int orderInGroup = 0;
        config.define(
                POLL_INTERVAL_MS_CONFIG,
                Type.STRING,
                POLL_INTERVAL_MS_DEFAULT,
                Importance.HIGH,
                POLL_INTERVAL_MS_DOC,
                CONNECTOR_GROUP,
                ++orderInGroup,
                Width.SHORT,
                POLL_INTERVAL_MS_DISPLAY
        ).define(
                BATCH_MAX_ROWS_CONFIG,
                Type.STRING,
                BATCH_MAX_ROWS_DEFAULT,
                Importance.LOW,
                BATCH_MAX_ROWS_DOC,
                CONNECTOR_GROUP,
                ++orderInGroup,
                Width.SHORT,
                BATCH_MAX_ROWS_DISPLAY
        ).define(
                TOPIC_CONFIG,
                Type.STRING,
                Importance.HIGH,
                TOPIC_DOC,
                CONNECTOR_GROUP,
                ++orderInGroup,
                Width.MEDIUM,
                TOPIC_DISPLAY
        );
    }

    private static void addLabelOptions(ConfigDef config) {
        int orderInGroup = 0;
        config.define(
                LABEL_KEY,
                Type.STRING,
                Importance.LOW,
                LABEL_KEY_DOC,
                LABELING_GROUP,
                ++orderInGroup,
                Width.LONG,
                LABEL_KEY_DISPLAY
        ).define(
                LABEL_VALUE,
                Type.STRING,
                Importance.LOW,
                LABEL_VALUE_DOC,
                LABELING_GROUP,
                ++orderInGroup,
                Width.LONG,
                LABEL_VALUE_DISPLAY
        );
    }

    public ElasticSourceConnectorConfig(Map<String, String> properties) {
        super(CONFIG_DEF, properties);
    }

    protected ElasticSourceConnectorConfig(ConfigDef subclassConfigDef, Map<String, String> props) {
        super(subclassConfigDef, props);
    }

    public static void main(String[] args) {
        System.out.println(CONFIG_DEF.toEnrichedRst());
    }
}
