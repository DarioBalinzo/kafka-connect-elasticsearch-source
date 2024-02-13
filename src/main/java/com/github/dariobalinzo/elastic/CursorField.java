package com.github.dariobalinzo.elastic;

import java.util.Map;

import static com.github.dariobalinzo.elastic.ElasticJsonNaming.removeKeywordSuffix;

public class CursorField {
    private final String cursor;

    public CursorField(String cursor) {
        this.cursor = removeKeywordSuffix(cursor);
    }

    public String read(Map<String, Object> document) {
        return read(document, cursor);
    }

    private String read(Map<String, Object> document, String field) {
        int firstDot = field.indexOf('.');

        Object value = null;
        if (document.containsKey(field)) {
            value = document.get(field);
        } else if (firstDot > 0 && firstDot < field.length() - 1) {
            String parent = field.substring(0, firstDot);
            Object nested = document.get(parent);
            if (nested instanceof Map) {
                return read((Map<String, Object>) document.get(parent),
                            field.substring(firstDot + 1));
            }
        }

        return value == null ? null : value.toString();
    }
}
