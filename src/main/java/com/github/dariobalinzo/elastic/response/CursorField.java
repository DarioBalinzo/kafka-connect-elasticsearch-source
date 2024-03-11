package com.github.dariobalinzo.elastic.response;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

public final class CursorField {

    private final String field;
    private final Object initialValue;


    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public CursorField(@JsonProperty("field") String field, @JsonProperty("initialValue") Object initialValue) {
        Objects.requireNonNull(field);
        this.field = field;
        this.initialValue = initialValue;
    }

    public String getField() {
        return field;
    }

    public Object getInitialValue() {
        return initialValue;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        var that = (CursorField) obj;
        return Objects.equals(this.field, that.field) && Objects.equals(this.initialValue, that.initialValue);
    }

    @Override
    public int hashCode() {
        return Objects.hash(field, initialValue);
    }

    @Override
    public String toString() {
        return "CursorField[" + "field=" + field + ", " + "initialValue=" + initialValue + ']';
    }
}
