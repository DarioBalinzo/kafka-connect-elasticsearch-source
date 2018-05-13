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

import org.apache.kafka.common.config.AbstractConfig;
import org.apache.kafka.common.config.ConfigDef;
import org.apache.kafka.common.config.ConfigDef.Importance;
import org.apache.kafka.common.config.ConfigDef.Type;
import org.apache.kafka.common.config.ConfigDef.Width;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Map;

public class ElasticSourceConnectorConfig extends AbstractConfig {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSourceConnectorConfig.class);

    //TODO add the possibility to specify multiple hosts
    public final static String ES_HOST_CONF = "es.host";
    private final static String ES_HOST_DOC = "ElasticSearch host";
    private final static String ES_HOST_DISPLAY = "Elastic host";


    public final static String ES_PORT_CONF = "es.port";
    private final static String ES_PORT_DOC = "ElasticSearch port";
    private final static String ES_PORT_DISPLAY = "ElasticSearch port";

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
    public static final int CONNECTION_ATTEMPTS_DEFAULT = 3;

    public static final String CONNECTION_BACKOFF_CONFIG = "connection.backoff.ms";
    private static final String CONNECTION_BACKOFF_DOC
            = "Backoff time in milliseconds between connection attempts.";
    private static final String CONNECTION_BACKOFF_DISPLAY
            = "Elastic connection backoff in milliseconds";
    public static final long CONNECTION_BACKOFF_DEFAULT = 10000L;

    public static final String POLL_INTERVAL_MS_CONFIG = "poll.interval.ms";
    private static final String POLL_INTERVAL_MS_DOC = "Frequency in ms to poll for new data in "
            + "each index.";
    public static final int POLL_INTERVAL_MS_DEFAULT = 5000;
    private static final String POLL_INTERVAL_MS_DISPLAY = "Poll Interval (ms)";

    public static final String BATCH_MAX_ROWS_CONFIG = "batch.max.rows";
    private static final String BATCH_MAX_ROWS_DOC =
            "Maximum number of record to include in a single batch when polling for new data.";
    public static final int BATCH_MAX_ROWS_DEFAULT = 100;
    private static final String BATCH_MAX_ROWS_DISPLAY = "Max Rows Per Batch";

    public static final String MODE_UNSPECIFIED = "";
    public static final String MODE_BULK = "bulk";
    public static final String MODE_TIMESTAMP = "timestamp";
    public static final String MODE_INCREMENTING = "incrementing";
    public static final String MODE_TIMESTAMP_INCREMENTING = "timestamp+incrementing";

    public static final String INCREMENTING_FIELD_NAME_CONFIG = "incrementing.field.name";
    private static final String INCREMENTING_FIELD_NAME_DOC =
            "The name of the strictly incrementing field to use to detect new records.";
    public static final String INCREMENTING_FIELD_NAME_DEFAULT = "";
    private static final String INCREMENTING_FIELD_NAME_DISPLAY = "Incrementing Field Name";

    public static final String TIMESTAMP_FIELD_NAME_CONFIG = "timestamp.field.name";
    private static final String TIMESTAMP_FIELD_NAME_DOC =
            "The name of the timestamp field to use to detect new or modified rows. This field may "
                    + "not be nullable.";
    public static final String TIMESTAMP_FIELD_NAME_DEFAULT = "";
    private static final String TIMESTAMP_FIELD_NAME_DISPLAY = "Timestamp Column Name";

    public static final String INDEX_PREFIX_CONFIG = "index.prefix";
    private static final String INDEX_PREFIX_DOC = "List of indices to include in copying.";
    public static final String INDEX_PREFIX_DEFAULT = "";
    private static final String INDEX_PREFIX_DISPLAY = "Indices prefix Whitelist";

    public static final String SCHEMA_PATTERN_CONFIG = "schema.pattern";
    private static final String SCHEMA_PATTERN_DOC =
            "Schema pattern to fetch indexs mapping from:\n"
                    + "  * \"\" retrieves those without a schema,"
                    + "  * null (default) means that the schema name should not be used to narrow the search, "
                    + "all indexs "
                    + "metadata would be fetched, regardless their schema.";
    private static final String SCHEMA_PATTERN_DISPLAY = "Schema pattern";

    public static final String TOPIC_PREFIX_CONFIG = "topic.prefix";
    private static final String TOPIC_PREFIX_DOC =
            "Prefix to prepend to index names to generate the name of the Kafka topic to publish data";
    private static final String TOPIC_PREFIX_DISPLAY = "Topic Prefix";

    public static final String DATABASE_GROUP = "Database";
    public static final String MODE_GROUP = "Mode";
    public static final String CONNECTOR_GROUP = "Connector";

    public static final String MODE_CONFIG = "mode";
    private static final String MODE_DOC = "";
    private static final String MODE_DISPLAY = "Index Loading Mode";

    public static final String INDICES_CONFIG = "es.indices";
    private static final String INDICES_DOC = "List of indices for this task to watch for changes.";

    public static final ConfigDef CONFIG_DEF = baseConfigDef();


    public static ConfigDef baseConfigDef() {
        ConfigDef config = new ConfigDef();
        addDatabaseOptions(config);
        addModeOptions(config);
        addConnectorOptions(config);
        return config;
    }

    private static final void addDatabaseOptions(ConfigDef config) {
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
                Arrays.asList(INDEX_PREFIX_CONFIG)
        ).define(
                ES_PORT_CONF,
                Type.STRING,
                Importance.HIGH,
                ES_PORT_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.LONG,
                ES_PORT_CONF,
                Arrays.asList(INDEX_PREFIX_CONFIG)
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
                Type.PASSWORD,
                null,
                Importance.HIGH,
                ES_PWD_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.SHORT,
                ES_PWD_DISPLAY
        ).define(
                CONNECTION_ATTEMPTS_CONFIG,
                Type.INT,
                CONNECTION_ATTEMPTS_DEFAULT,
                Importance.LOW,
                CONNECTION_ATTEMPTS_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                ConfigDef.Width.SHORT,
                CONNECTION_ATTEMPTS_DISPLAY
        ).define(
                CONNECTION_BACKOFF_CONFIG,
                Type.LONG,
                CONNECTION_BACKOFF_DEFAULT,
                Importance.LOW,
                CONNECTION_BACKOFF_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.SHORT,
                CONNECTION_BACKOFF_DISPLAY
        ).define(
                INDEX_PREFIX_CONFIG,
                Type.LIST,
                INDEX_PREFIX_CONFIG,
                Importance.MEDIUM,
                INDEX_PREFIX_CONFIG,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.LONG,
                INDEX_PREFIX_CONFIG
        ).define(
                SCHEMA_PATTERN_CONFIG,
                Type.STRING,
                null,
                Importance.MEDIUM,
                SCHEMA_PATTERN_DOC,
                DATABASE_GROUP,
                ++orderInGroup,
                Width.SHORT,
                SCHEMA_PATTERN_DISPLAY
        );
    }

    private static final void addModeOptions(ConfigDef config) {
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
                Arrays.asList(
                        INCREMENTING_FIELD_NAME_CONFIG,
                        TIMESTAMP_FIELD_NAME_CONFIG
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
        ).define(
                TIMESTAMP_FIELD_NAME_CONFIG,
                Type.STRING,
                TIMESTAMP_FIELD_NAME_DEFAULT,
                Importance.MEDIUM,
                TIMESTAMP_FIELD_NAME_DOC,
                MODE_GROUP,
                ++orderInGroup,
                Width.MEDIUM,
                TIMESTAMP_FIELD_NAME_DISPLAY
        );
    }

    private static final void addConnectorOptions(ConfigDef config) {
        int orderInGroup = 0;
        config.define(
                POLL_INTERVAL_MS_CONFIG,
                Type.INT,
                POLL_INTERVAL_MS_DEFAULT,
                Importance.HIGH,
                POLL_INTERVAL_MS_DOC,
                CONNECTOR_GROUP,
                ++orderInGroup,
                Width.SHORT,
                POLL_INTERVAL_MS_DISPLAY
        ).define(
                BATCH_MAX_ROWS_CONFIG,
                Type.INT,
                BATCH_MAX_ROWS_DEFAULT,
                Importance.LOW,
                BATCH_MAX_ROWS_DOC,
                CONNECTOR_GROUP,
                ++orderInGroup,
                Width.SHORT,
                BATCH_MAX_ROWS_DISPLAY
        ).define(
                TOPIC_PREFIX_CONFIG,
                Type.STRING,
                Importance.HIGH,
                TOPIC_PREFIX_DOC,
                CONNECTOR_GROUP,
                ++orderInGroup,
                Width.MEDIUM,
                TOPIC_PREFIX_DISPLAY
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
