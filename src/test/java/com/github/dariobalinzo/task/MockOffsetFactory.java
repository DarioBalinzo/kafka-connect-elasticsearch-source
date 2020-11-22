package com.github.dariobalinzo.task;

import org.apache.kafka.connect.storage.OffsetStorageReader;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

public class MockOffsetFactory {

    static OffsetStorageReader empty() {
        return emptyOffset;
    }

    static OffsetStorageReader from(String initialCursor) {
        Map<String, Object> state = new HashMap<>();
        state.put(ElasticSourceTask.POSITION, initialCursor);

        return new OffsetStorageReader() {
            @Override
            public <T> Map<String, Object> offset(Map<String, T> map) {
                return state;
            }

            @Override
            public <T> Map<Map<String, T>, Map<String, Object>> offsets(Collection<Map<String, T>> collection) {
                return null;
            }
        };
    }

    private static OffsetStorageReader emptyOffset = new OffsetStorageReader() {
        @Override
        public <T> Map<String, Object> offset(Map<String, T> map) {
            return new HashMap<>();
        }

        @Override
        public <T> Map<Map<String, T>, Map<String, Object>> offsets(Collection<Map<String, T>> collection) {
            return null;
        }
    };

}
