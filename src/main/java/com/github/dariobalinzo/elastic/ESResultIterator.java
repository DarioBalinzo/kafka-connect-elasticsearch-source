package com.github.dariobalinzo.elastic;

import co.elastic.clients.elasticsearch.ElasticsearchClient;
import co.elastic.clients.elasticsearch._types.FieldSort;
import co.elastic.clients.elasticsearch._types.SortOptions;
import co.elastic.clients.elasticsearch._types.query_dsl.BoolQuery;
import co.elastic.clients.elasticsearch._types.query_dsl.Query;
import co.elastic.clients.elasticsearch.core.ClosePointInTimeRequest;
import co.elastic.clients.elasticsearch.core.OpenPointInTimeRequest;
import co.elastic.clients.elasticsearch.core.SearchRequest;
import co.elastic.clients.elasticsearch.core.SearchResponse;
import co.elastic.clients.elasticsearch.core.search.Hit;
import co.elastic.clients.elasticsearch.core.search.PointInTimeReference;
import co.elastic.clients.json.JsonData;
import co.elastic.clients.util.ObjectBuilder;
import com.github.dariobalinzo.elastic.response.Cursor;
import com.github.dariobalinzo.task.ElasticSourceTask;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.*;

import static java.util.stream.Collectors.toMap;


public class ESResultIterator implements Iterable<ESResultIterator.Record>, Iterator<ESResultIterator.Record>, AutoCloseable {

    private final int pitKeepAliveSeconds;
    private Cursor cursor;
    private Iterator<Hit<Map<String, JsonData>>> page = Collections.emptyIterator();
    private final ElasticsearchClient client;
    private final int pageSize;
    private final Query query;


    private static final Logger logger = LoggerFactory.getLogger(ElasticSourceTask.class);

    public ESResultIterator(
        ElasticsearchClient client,
        Cursor cursor,
        int pageSize,
        int pitKeepAliveSeconds
    ) {
        this.pitKeepAliveSeconds = pitKeepAliveSeconds;
        logger.debug("Creating ESResultIterator with cursor: {}", cursor);
        this.client = client;
        this.pageSize = pageSize;
        this.cursor = maybeOpenPit(cursor, pitKeepAliveSeconds);

        var queryBuilder = new BoolQuery.Builder();
        for (var cursorField : cursor.getCursorFields()) {
            var must = new BoolQuery.Builder().must(
                m -> m.range(
                    r -> {
                        var field = r.field(cursorField.getField());
                        var initialValue =
                            cursorField.getInitialValue() == null ? null : JsonData.of(cursorField.getInitialValue());

                        if (cursor.isIncludeLower())
                            return field.gte(initialValue);

                        return field.gt(initialValue);
                    }
                )
            );
            queryBuilder = queryBuilder.must(m -> m.bool(must.build()));
        }
        this.query = new Query.Builder().bool(queryBuilder.build()).build();
        logger.debug("Iterator query is: {}", this.query);
    }

    @Override
    public boolean hasNext() {
        try {
            if (this.page.hasNext())
                return true;
            this.loadNextPage();
            if (this.page.hasNext())
                return true;
            else {
                this.close();
                return false;
            }
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public ESResultIterator.Record next() {
        if (!this.hasNext())
            throw new NoSuchElementException();

        var next = this.page.next();
        this.cursor = this.cursor.withSortValues(next.sort());
        return new Record(next.id(), this.cursor.getIndex(), next.source());
    }

    @Override
    public Iterator<Record> iterator() {
        return this;
    }

    private Cursor maybeOpenPit(Cursor cursor, int pitKeepAliveSeconds) {
        if (cursor.getPitId() != null) {
            return cursor;
        }

        var openRequest = new OpenPointInTimeRequest.Builder()
            .index(cursor.getIndex())
            .keepAlive(t -> t.time(pitKeepAliveSeconds + "s"))
            .build();
        try {
            var openResponse = this.client.openPointInTime(openRequest);
            return cursor.withPitId(openResponse.id());
        } catch (IOException e) {
            // just throw. it should come back around from the framework and try again x times
            throw new RuntimeException(e);
        }
    }

    private void loadNextPage() throws Exception {
        SearchResponse<Map<String, JsonData>> response = this.client.search(this::buildSearchRequest, (Type) Map.class);
        this.page = response.hits().hits().iterator();
    }

    private ObjectBuilder<SearchRequest> buildSearchRequest(SearchRequest.Builder s) {
        s.pit(new PointInTimeReference.Builder()
                .id(this.cursor.getPitId())
                .keepAlive(t -> t.time(pitKeepAliveSeconds + "s")).build()
            )
            .query(query)
            .sort((SortOptions.Builder sob) -> {
                cursor.getCursorFields().stream().map(cursorField ->
                    FieldSort.of(f ->
                        f.field(cursorField.getField()))).forEachOrdered(sob::field);
                return sob;
            })
            .size(this.pageSize);

        if (this.cursor.getSortValues() != null) {
            s.searchAfter(this.cursor.getSortValues());
        }

        return s;
    }

    public Cursor getCursor() {
        return this.cursor;
    }

    @Override
    public void close() throws Exception {
        if (this.cursor.getPitId() != null) {
            var closeRequest = new ClosePointInTimeRequest.Builder()
                .id(this.cursor.getPitId())
                .build();
            try {
                logger.debug("Closing PIT with id: {} ", this.cursor.getPitId());
                this.client.closePointInTime(closeRequest);

            } catch (IOException e) {
                // just throw. it should come back around from the framework and try again x times
                throw new RuntimeException(e);
            }

            // don't include lower here, should have fully completed the iteration
            this.cursor = this.cursor.reframe(false);
        }
    }

    public static class Record {
        private final Map<String, Object> record;
        private final String id;

        private final String index;

        public Record(String id, String index, Map<String, JsonData> record) {

            if (record == null) {
                this.record = Collections.emptyMap();
                this.id = null;
                this.index = null;
            } else {
                this.record = record.entrySet().stream()
                    .filter(entry -> entry.getValue() != null)
                    .collect(
                        toMap(Map.Entry::getKey,
                            Map.Entry::getValue,
                            (a, b) -> b, HashMap::new));

                this.id = id;
                this.index = index;
                this.record.put("es-id", id);
                this.record.put("es-index", index);
            }
        }

        public Map<String, Object> getData() {
            return record;
        }

        public String getId() {
            return this.id;
        }

        public String getIndex() {
            return this.index;
        }
    }
}
