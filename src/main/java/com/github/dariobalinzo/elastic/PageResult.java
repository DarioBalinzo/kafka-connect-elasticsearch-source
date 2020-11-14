package com.github.dariobalinzo.elastic;

import java.util.List;
import java.util.Map;

public class PageResult {
    private final String index;
    private final List<Map<String, Object>> documents;
    private final String lastCursor;

    public PageResult(String index, List<Map<String, Object>> documents, String cursorField) {
        this.index = index;
        this.documents = documents;
        if (documents.isEmpty()) {
            this.lastCursor = null;
        } else {
            Map<String, Object> lastDocument = documents.get(documents.size() - 1);
            this.lastCursor = lastDocument.get(cursorField).toString();
        }
    }

    public List<Map<String, Object>> getDocuments() {
        return documents;
    }

    public String getLastCursor() {
        return lastCursor;
    }

    public String getIndex() {
        return index;
    }
}
