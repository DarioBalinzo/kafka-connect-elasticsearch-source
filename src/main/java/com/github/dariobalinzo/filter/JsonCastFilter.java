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
package com.github.dariobalinzo.filter;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Map;
import java.util.Set;

public class JsonCastFilter implements DocumentFilter {
    private final Set<String> fieldsToCast;
    private final JsonFilterVisitor visitor;
    private final ObjectMapper objectMapper = new ObjectMapper();

    public JsonCastFilter(Set<String> fieldsToCast) {
        this.fieldsToCast = fieldsToCast;
        visitor = new JsonFilterVisitor(this::checkIfJsonCastNeeded);
    }

    @Override
    public void filter(Map<String, Object> document) {
        visitor.visit(document);
    }

    private Object checkIfJsonCastNeeded(String key, Object value) {
        if (fieldsToCast.contains(key)) {
            return castToJson(value);
        } else {
            return value;
        }
    }

    private String castToJson(Object value) {
        try {
            return objectMapper.writeValueAsString(value);
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
