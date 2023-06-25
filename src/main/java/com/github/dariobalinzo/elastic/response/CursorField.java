package com.github.dariobalinzo.elastic.response;

import java.util.Objects;

public record CursorField(String field, Object initialValue) {
    public CursorField{
        Objects.requireNonNull(field);
        Objects.requireNonNull(initialValue);
    }
}
