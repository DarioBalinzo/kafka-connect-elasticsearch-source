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

import java.util.Iterator;
import java.util.List;
import java.util.Map;

public class JsonFilterVisitor {
    private final JsonElementFilter businessLogic;

    public JsonFilterVisitor(JsonElementFilter businessLogic) {
        this.businessLogic = businessLogic;
    }

    public void visit(Map<String, Object> document) {
        visitJsonDocument("", document);
    }

    @SuppressWarnings("unchecked")
    private void visitJsonDocument(String prefixPathName, Map<String, Object> document) {
        Iterator<Map.Entry<String, Object>> iterator = document.entrySet().iterator();
        while (iterator.hasNext()) {
            Map.Entry<String, Object> entry = iterator.next();
            String fullPathKey = prefixPathName + entry.getKey();
            Object element = businessLogic.filterElement(fullPathKey, entry.getValue());
            if (element == null) {
                iterator.remove();
            } else {
                entry.setValue(element);
            }

            if (entry.getValue() instanceof List) {
                List<Object> nestedList = (List<Object>) entry.getValue();
                visitNestedList(fullPathKey + ".", nestedList);
            } else if (entry.getValue() instanceof Map) {
                String nestedObjectPath = prefixPathName + entry.getKey() + ".";
                visitJsonDocument(nestedObjectPath, (Map<String, Object>) entry.getValue());
            }
        }
    }

    private void visitNestedList(String prefixPathName, List<Object> nestedList) {
        nestedList.forEach(item -> visitNestedMap(prefixPathName, item));
    }

    @SuppressWarnings("unchecked")
    private void visitNestedMap(String prefixPathName, Object item) {
        if (item instanceof Map) {
            visitJsonDocument(prefixPathName, (Map<String, Object>) item);
        }
    }

}
