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

import java.util.List;
import java.util.Map;
import java.util.Set;

public class BlacklistFilter implements DocumentFilter {
    private final JsonFilterVisitor visitor;
    private final Set<String> fieldsToRemove;

    public BlacklistFilter(Set<String> fieldsToRemove) {
        this.fieldsToRemove = fieldsToRemove;
        visitor = new JsonFilterVisitor(this::filterBlacklistItem);
    }

    private Object filterBlacklistItem(String key, Object value) {
        if (value instanceof Map || value instanceof List) {
            boolean shouldVisitNestedObj = fieldsToRemove.stream()
                    .anyMatch(jsonPath -> jsonPath.startsWith(key));
            return shouldVisitNestedObj ? value : null;
        }
        return fieldsToRemove.contains(key) ? null : value;
    }

    @Override
    public void filter(Map<String, Object> document) {
        visitor.visit(document);
    }
}
