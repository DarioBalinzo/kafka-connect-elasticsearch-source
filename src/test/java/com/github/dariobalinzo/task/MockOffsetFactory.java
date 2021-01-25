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
        return from(initialCursor, null);
    }

    static OffsetStorageReader from(String initialCursor, String secondaryCursor) {
        Map<String, Object> state = new HashMap<>();
        state.put(ElasticSourceTask.POSITION, initialCursor);
        if (secondaryCursor != null) {
            state.put(ElasticSourceTask.POSITION_SECONDARY, secondaryCursor);
        }

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
