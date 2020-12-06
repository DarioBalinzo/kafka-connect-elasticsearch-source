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
            Object element = businessLogic.filterElement(entry.getKey(), entry.getValue());
            if (element == null) {
                iterator.remove();
            } else {
                entry.setValue(element);
            }

            if (entry.getValue() instanceof List) {
                List<Object> nestedList = (List<Object>) entry.getValue();
                visitNestedList(prefixPathName + ".", nestedList);
            } else if (entry.getValue() instanceof Map) {
                String nestedObjectPath = prefixPathName + "." + entry.getKey() + ".";
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
