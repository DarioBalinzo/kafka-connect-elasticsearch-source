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
import com.github.dariobalinzo.elastic.ElasticConnection;
import com.github.dariobalinzo.elastic.ElasticConnectionBuilder;
import com.github.dariobalinzo.elastic.ElasticRepository;
import com.github.dariobalinzo.elastic.response.Cursor;
import com.github.dariobalinzo.filter.BlacklistFilter;
import com.github.dariobalinzo.filter.DocumentFilter;
import com.github.dariobalinzo.filter.JsonCastFilter;
import com.github.dariobalinzo.filter.WhitelistFilter;
import com.github.dariobalinzo.schema.*;
import org.apache.kafka.common.config.ConfigException;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
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


    private final OffsetSerializer offsetSerializer = new OffsetSerializer();
    private SchemaConverter schemaConverter;
    private StructConverter structConverter;

    private ElasticSourceTaskConfig config;
    private ElasticConnection es;

    private final AtomicBoolean stopping = new AtomicBoolean(false);
    private List<String> indices;
    private String topic;
//    private CursorFields cursorFields;
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
//        try {
//            config = new ElasticSourceTaskConfig(properties);
//        } catch (ConfigException e) {
//            throw new ConnectException("Couldn't start ElasticSourceTask due to configuration error", e);
//        }
//
//        indices = Arrays.asList(config.getString(ElasticSourceTaskConfig.INDICES_CONFIG).split(","));
//        if (indices.isEmpty()) {
//            throw new ConnectException("Invalid configuration: each ElasticSourceTask must have at "
//                    + "least one index assigned to it");
//        }
//
//        topic = config.getString(ElasticSourceConnectorConfig.TOPIC_PREFIX_CONFIG);
//        String primaryCursorField = config.getString(ElasticSourceConnectorConfig.INCREMENTING_FIELD_NAME_CONFIG);
//        Objects.requireNonNull(primaryCursorField, ElasticSourceConnectorConfig.INCREMENTING_FIELD_NAME_CONFIG
//                + " conf is mandatory");
//        cursorFields = new CursorFields(primaryCursorField, config.getString(ElasticSourceConnectorConfig.SECONDARY_INCREMENTING_FIELD_NAME_CONFIG));
//        pollingMs = Integer.parseInt(config.getString(ElasticSourceConnectorConfig.POLL_INTERVAL_MS_CONFIG));
//
//        initConnectorFilters();
//        initConnectorFieldConverter();
//        initEsConnection();
    }

    private void initConnectorFilters() {
        String whiteFilters = config.getString(ElasticSourceConnectorConfig.FIELDS_WHITELIST_CONFIG);
        if (whiteFilters != null) {
            String[] whiteFiltersArray = whiteFilters.split(";");
            Set<String> whiteFiltersSet = new HashSet<>(Arrays.asList(whiteFiltersArray));
            documentFilters.add(new WhitelistFilter(whiteFiltersSet));
        }

        String blackFilters = config.getString(ElasticSourceConnectorConfig.FIELDS_BLACKLIST_CONFIG);
        if (blackFilters != null) {
            String[] blackFiltersArray = blackFilters.split(";");
            Set<String> blackFiltersSet = new HashSet<>(Arrays.asList(blackFiltersArray));
            documentFilters.add(new BlacklistFilter(blackFiltersSet));
        }

        String jsonCastFilters = config.getString(ElasticSourceConnectorConfig.FIELDS_JSON_CAST_CONFIG);
        if (jsonCastFilters != null) {
            String[] jsonCastFiltersArray = jsonCastFilters.split(";");
            Set<String> whiteFiltersSet = new HashSet<>(Arrays.asList(jsonCastFiltersArray));
            documentFilters.add(new JsonCastFilter(whiteFiltersSet));
        }
    }

    private void initConnectorFieldConverter() {
        String nameConverterConfig = config.getString(ElasticSourceConnectorConfig.CONNECTOR_FIELDNAME_CONVERTER_CONFIG);

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
        String esScheme = config.getString(ElasticSourceConnectorConfig.ES_SCHEME_CONF);
        String esHost = config.getString(ElasticSourceConnectorConfig.ES_HOST_CONF);
        int esPort = Integer.parseInt(config.getString(ElasticSourceConnectorConfig.ES_PORT_CONF));

        String esUser = config.getString(ElasticSourceConnectorConfig.ES_USER_CONF);
        String esPwd = config.getString(ElasticSourceConnectorConfig.ES_PWD_CONF);

        int batchSize = Integer.parseInt(config.getString(ElasticSourceConnectorConfig.BATCH_MAX_ROWS_CONFIG));

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
            es = connectionBuilder.build();
        } else {
            es = connectionBuilder.withUser(esUser)
                    .withPassword(esPwd)
                    .build();
        }

        elasticRepository = new ElasticRepository(es);
        elasticRepository.setPageSize(batchSize);
    }


    //will be called by connect with a different thread than the stop thread
    @Override
    public List<SourceRecord> poll() {
//        List<SourceRecord> results = new ArrayList<>();
//        try {
//            for (String index : indices) {
//                if (!stopping.get()) {
//                    logger.info("fetching from {}", index);
//                    Cursor lastValue = fetchLastOffset(index);
//                    logger.info("found last initialValue {}", lastValue);
//
//                    try (ElasticPointInTimeResource pointInTime = elasticRepository.newPointTime(index)) {
//                        PageResult pageResult = cursorFields.getCursorType() == CursorType.PRIMARY_ONLY ?
//                                elasticRepository.searchAfter(index, lastValue, pointInTime.getPitId()) :
//                                elasticRepository.searchAfterWithSecondarySort(index, lastValue, pointInTime.getPitId());
//                        parseResult(pageResult, results);
//                        logger.info("index {} total messages: {} ", index, sent.get(index));
//                    }
//                }
//            }
//            if (results.isEmpty()) {
//                logger.info("no data found, sleeping for {} ms", pollingMs);
//                Thread.sleep(pollingMs);
//            }
//
//        } catch (Exception e) {
//            logger.error("error", e);
//        }
//        return results;

        return List.of();
    }

//    private Cursor fetchLastOffset(String index) {
//        //first we check in cache memory the last initialValue
//        if (cursorCache.get(index) != null) {
//            return cursorCache.get(index);
//        }
//
//        //if cache is empty we check the framework
//        Map<String, Object> offset = context.offsetStorageReader().offset(Collections.singletonMap(INDEX, index));
//        if (offset != null) {
//            String primaryCursor = (String) offset.get(POSITION);
//            String secondaryCursor = (String) offset.get(POSITION_SECONDARY);
//            Object[] searchAfter = (Object[]) offset.get(SEARCH_AFTER);
//            return cursorFields.newCursor(primaryCursor, secondaryCursor, null, null);
//        } else {
//            return cursorFields.newEmptyCursor();
//        }
//
//        return null;
//    }
//
//    private void parseResult(PageResult pageResult, List<SourceRecord> results) {
//        String index = pageResult.index();
//        for (Map<String, Object> elasticDocument : pageResult.documents()) {
//            Map<String, String> sourcePartition = Collections.singletonMap(INDEX, index);
//            var sourceOffset = new CursorSerde().serialize(pageResult.cursor().);
//            String key = offsetSerializer.toStringOffset(
//                    cursorFields.getPrimaryCursorFieldJsonName(),
//                    cursorFields.getSecondaryCursorFieldJsonName(),
//                    index,
//                    elasticDocument
//            );
//
//            cursor.put(index, pageResult.getLastCursor());
//            sent.merge(index, 1, Integer::sum);
//
//            documentFilters.forEach(jsonFilter -> jsonFilter.filter(elasticDocument));
//
//            Schema schema = schemaConverter.convert(elasticDocument, index);
//            Struct struct = structConverter.convert(elasticDocument, schema);
//
//            SourceRecord sourceRecord = new SourceRecord(
//                    sourcePartition,
//                    sourceOffset,
//                    topic + index,
//                    //KEY
//                    Schema.STRING_SCHEMA,
//                    key,
//                    //VALUE
//                    schema,
//                    struct);
//            results.add(sourceRecord);
//        }
//    }

    //will be called by connect with a different thread than poll thread
    public void stop() {
        stopping.set(true);
        if (es != null) {
            es.closeQuietly();
        }
    }
}
