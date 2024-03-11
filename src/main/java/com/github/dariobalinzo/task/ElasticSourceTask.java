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

package com.github.dariobalinzo.task;

import com.github.dariobalinzo.ElasticSourceConnectorConfig;
import com.github.dariobalinzo.Version;
import com.github.dariobalinzo.elastic.ESResultIterator;
import com.github.dariobalinzo.elastic.ElasticConnection;
import com.github.dariobalinzo.elastic.ElasticConnectionBuilder;
import com.github.dariobalinzo.elastic.ElasticRepository;
import com.github.dariobalinzo.elastic.response.Cursor;
import com.github.dariobalinzo.elastic.response.CursorField;
import com.github.dariobalinzo.filter.BlacklistFilter;
import com.github.dariobalinzo.filter.DocumentFilter;
import com.github.dariobalinzo.filter.JsonCastFilter;
import com.github.dariobalinzo.filter.WhitelistFilter;
import com.github.dariobalinzo.schema.*;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.errors.ConnectException;
import org.apache.kafka.connect.source.SourceRecord;
import org.apache.kafka.connect.source.SourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;

public class ElasticSourceTask extends SourceTask {

    private static final Logger logger = LoggerFactory.getLogger(ElasticSourceTask.class);
    private static final String INDEX = "index";
    static final String POSITION = "position";
    static final String POSITION_SECONDARY = "position_secondary";

    static final String SEARCH_AFTER = "search_after";


    private SchemaConverter schemaConverter;
    private StructConverter structConverter;

    private ElasticSourceTaskConfig config;
    private ElasticConnection es;

    private final AtomicBoolean stopping = new AtomicBoolean(false);
    private List<String> indices;
    private String topic;

    // cursorFields order is important
    private final List<CursorField> cursorFields = new ArrayList<>();
    private int pollingMs;
    private final Map<String, Cursor> cursorCache = new HashMap<>();
    private final Map<String, Integer> sent = new HashMap<>();
    private ElasticRepository elasticRepository;

    private final List<DocumentFilter> documentFilters = new ArrayList<>();

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

        indices = Arrays.asList(config.getString(ElasticSourceTaskConfig.INDICES_CONFIG).split(","));
        if (indices.isEmpty()) {
            throw new ConnectException("Invalid configuration: each ElasticSourceTask must have at "
                + "least one index assigned to it");
        }

        topic = config.getString(ElasticSourceConnectorConfig.TOPIC_PREFIX_CONFIG);
        var incrementingFieldName = config.getString(ElasticSourceConnectorConfig.INCREMENTING_FIELD_NAME_CONFIG);
        Objects.requireNonNull(incrementingFieldName, ElasticSourceConnectorConfig.INCREMENTING_FIELD_NAME_CONFIG
            + " conf is mandatory");
        var primaryCursorField = new CursorField(incrementingFieldName,
            config.getString(ElasticSourceConnectorConfig.INCREMENTING_FIELD_INITIAL_VALUE_CONFIG));
        cursorFields.add(primaryCursorField);

        var secondaryIncrementingFieldName = config.getString(ElasticSourceConnectorConfig.SECONDARY_INCREMENTING_FIELD_NAME_CONFIG);
        if (secondaryIncrementingFieldName != null) {
            var secondaryCursorField = new CursorField(secondaryIncrementingFieldName,
                config.getString(ElasticSourceConnectorConfig.SECONDARY_INCREMENTING_FIELD_INITIAL_VALUE_CONFIG));
            cursorFields.add(secondaryCursorField);
        }

        pollingMs = Integer.parseInt(config.getString(ElasticSourceConnectorConfig.POLL_INTERVAL_MS_CONFIG));

        initConnectorFilters();
        initConnectorFieldConverter();
        initEsConnection();
    }

    private void initConnectorFilters() {
        var whiteFilters = config.getString(ElasticSourceConnectorConfig.FIELDS_WHITELIST_CONFIG);
        if (whiteFilters != null) {
            var whiteFiltersArray = whiteFilters.split(";");
            Set<String> whiteFiltersSet = new HashSet<>(Arrays.asList(whiteFiltersArray));
            documentFilters.add(new WhitelistFilter(whiteFiltersSet));
        }

        var blackFilters = config.getString(ElasticSourceConnectorConfig.FIELDS_BLACKLIST_CONFIG);
        if (blackFilters != null) {
            var blackFiltersArray = blackFilters.split(";");
            Set<String> blackFiltersSet = new HashSet<>(Arrays.asList(blackFiltersArray));
            documentFilters.add(new BlacklistFilter(blackFiltersSet));
        }

        var jsonCastFilters = config.getString(ElasticSourceConnectorConfig.FIELDS_JSON_CAST_CONFIG);
        if (jsonCastFilters != null) {
            var jsonCastFiltersArray = jsonCastFilters.split(";");
            Set<String> whiteFiltersSet = new HashSet<>(Arrays.asList(jsonCastFiltersArray));
            documentFilters.add(new JsonCastFilter(whiteFiltersSet));
        }
    }

    private void initConnectorFieldConverter() {
        var nameConverterConfig = config.getString(ElasticSourceConnectorConfig.CONNECTOR_FIELDNAME_CONVERTER_CONFIG);

        FieldNameConverter fieldNameConverter;
        switch (nameConverterConfig) {
            case ElasticSourceConnectorConfig.NOP_FIELDNAME_CONVERTER:
                fieldNameConverter = new NopNameConverter();
                break;
            case ElasticSourceConnectorConfig.AVRO_FIELDNAME_CONVERTER:
            default:
                fieldNameConverter = new AvroName();
                break;
        }
        this.schemaConverter = new SchemaConverter(fieldNameConverter);
        this.structConverter = new StructConverter(fieldNameConverter);
    }

    private void initEsConnection() {
        var esScheme = config.getString(ElasticSourceConnectorConfig.ES_SCHEME_CONF);
        var esHost = config.getString(ElasticSourceConnectorConfig.ES_HOST_CONF);
        var esPort = Integer.parseInt(config.getString(ElasticSourceConnectorConfig.ES_PORT_CONF));

        var esUser = config.getString(ElasticSourceConnectorConfig.ES_USER_CONF);
        var esPwd = config.getString(ElasticSourceConnectorConfig.ES_PWD_CONF);

        var batchSize = Integer.parseInt(config.getString(ElasticSourceConnectorConfig.BATCH_MAX_ROWS_CONFIG));

        var maxConnectionAttempts = Integer.parseInt(config.getString(
            ElasticSourceConnectorConfig.CONNECTION_ATTEMPTS_CONFIG
        ));
        var connectionRetryBackoff = Long.parseLong(config.getString(
            ElasticSourceConnectorConfig.CONNECTION_BACKOFF_CONFIG
        ));
        var connectionBuilder = new ElasticConnectionBuilder(esHost, esPort)
            .withProtocol(esScheme)
            .withMaxAttempts(maxConnectionAttempts)
            .withBackoff(connectionRetryBackoff);

        var truststore = config.getString(ElasticSourceConnectorConfig.ES_TRUSTSTORE_CONF);
        var truststorePass = config.getString(ElasticSourceConnectorConfig.ES_TRUSTSTORE_PWD_CONF);
        var keystore = config.getString(ElasticSourceConnectorConfig.ES_KEYSTORE_CONF);
        var keystorePass = config.getString(ElasticSourceConnectorConfig.ES_KEYSTORE_PWD_CONF);

        if (truststore != null) {
            connectionBuilder.withTrustStore(truststore, truststorePass);
        }

        if (keystore != null) {
            connectionBuilder.withKeyStore(keystore, keystorePass);
        }

        if (esUser == null || esUser.isEmpty()) {
            es = connectionBuilder.build();
        } else {
            es = connectionBuilder.withUser(esUser)
                .withPassword(esPwd)
                .build();
        }

        var pitTimeout = config.getInt(ElasticSourceConnectorConfig.ES_POINT_IN_TIME_TIMEOUT_SECONDS_CONFIG);

        elasticRepository = new ElasticRepository(es, batchSize, pitTimeout);
    }


    //will be called by connect with a different thread than the stop thread
    @Override
    public List<SourceRecord> poll() {
        List<SourceRecord> results = new ArrayList<>();
        try {
            for (var index : indices) {
                if (!stopping.get()) {
                    logger.info("fetching from {}", index);
                    var cursor = fetchAndAlignLastOffset(index, cursorFields);
                    logger.info("found last initialValue {}", cursor);
                    try (var iterator = elasticRepository.getIterator(cursor)) {
                        var pair = parseResult(index, iterator);
                        results.addAll(pair.getLhs());
                        cursorCache.put(index, pair.getRhs().reframe());
                    }
                    logger.info("index {} total messages: {} ", index, sent.get(index));
                }
            }
            if (results.isEmpty()) {
                logger.info("no data found, sleeping for {} ms", pollingMs);
                Thread.sleep(pollingMs);
            }

        } catch (Exception e) {
            logger.error("error", e);
        }
        return results;
    }

    private Cursor fetchAndAlignLastOffset(String index, List<CursorField> cursorFields) {
        // first we check in cache memory the last initialValue
        if (cursorCache.get(index) != null) {
            return cursorCache.get(index);
        }

        // if cache is empty we check the framework
        var offset = context.offsetStorageReader().offset(Collections.singletonMap(INDEX, index));
        logger.info("offset from framework: {}", offset);
        if (offset == null || offset.isEmpty()) {
            return Cursor.of(index, cursorFields);
        }

        var cursor = new OffsetSerializer().deserialize(offset);
        if (cursor == null) {
            return Cursor.of(index, cursorFields);
        }

        return cursor;
    }

    private Pair<List<SourceRecord>, Cursor> parseResult(String index, ESResultIterator iterator) {
        var results = new ArrayList<SourceRecord>();
        while (iterator.hasNext() && !stopping.get()) {
            var record = iterator.next();
            var sourcePartition = Collections.singletonMap(INDEX, index);
            var sourceOffset = new OffsetSerializer().serialize(record.getCursor());

            sent.merge(index, 1, Integer::sum);

            var docMap = record.getData();
            for (DocumentFilter jsonFilter : documentFilters) {
                jsonFilter.filter(docMap);
            }
            var schema = schemaConverter.convert(docMap, index);
            var struct = structConverter.convert(docMap, schema);

            results.add(new SourceRecord(
                sourcePartition,
                sourceOffset,
                topic + index,
                //KEY
                Schema.STRING_SCHEMA,
                record.getId(),
                //VALUE
                schema,
                struct));
        }

        // return results and the cursor for the last record returned
        return new Pair<>(results, iterator.getCursor());
    }

    private static class Pair<L, R> {
        private final L lhs;
        private final R rhs;


        private Pair(L lhs, R rhs) {
            this.lhs = lhs;
            this.rhs = rhs;
        }

        public L getLhs() {
            return lhs;
        }

        public R getRhs() {
            return rhs;
        }

    }

    //will be called by connect with a different thread than poll thread
    public void stop() {
        stopping.set(true);
        if (es != null) {
            es.closeQuietly();
        }
    }
}
