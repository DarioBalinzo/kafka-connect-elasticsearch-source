package com.github.dariobalinzo.elastic.response;

import co.elastic.clients.elasticsearch._types.FieldValue;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;


public class Cursor {

    private final String index;
    private final List<CursorField> cursorFields;
    private final String pitId;
    private final List<FieldValue> sortValues;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public Cursor(@JsonProperty("index") String index,
                  @JsonProperty("cursorFields") List<CursorField> cursorFields,
                  @JsonProperty("pitId") String pitId,
                  @JsonProperty("sortValues") List<FieldValue> sortValues) {
        Objects.requireNonNull(index);
        Objects.requireNonNull(cursorFields);
        cursorFields = Collections.unmodifiableList(cursorFields);
        this.index = index;
        this.cursorFields = cursorFields;
        this.pitId = pitId;
        this.sortValues = sortValues;
    }

    public static Cursor of(String index, List<CursorField> cursorFields) {
        return new Cursor(index, cursorFields, null, null);
    }

    public String getIndex() {
        return index;
    }

    public List<CursorField> getCursorFields() {
        return cursorFields;
    }

    public String getPitId() {
        return pitId;
    }

    public List<FieldValue> getSortValues() {
        return sortValues;
    }

    public Cursor withPitId(String pitId) {
        return new Cursor(this.index, this.cursorFields, pitId, this.sortValues);
    }

    public Cursor withSortValues(List<FieldValue> sortValues) {
        return new Cursor(this.index, this.cursorFields, this.pitId, sortValues);
    }


    /**
     * Re-frames the current cursor into a new one with any sort values transferred to cursor fields as initial values
     * and no pitId.
     *
     * @return a new Cursor instance.
     */
    public Cursor reframe() {
        final List<CursorField> newCursorFields;
        if (sortValues == null || sortValues.isEmpty()) {

            // cycle the current cursorFields back
            newCursorFields = cursorFields;
        } else {

            // copy the sort values into the cursorFields as initial values
            newCursorFields = new ArrayList<>(cursorFields.size());

            for (int i = 0; i < cursorFields.size(); i++) {
                newCursorFields.add(new CursorField(cursorFields.get(i).getField(), this.sortValues.get(i)));
            }
        }

        return new Cursor(this.index, newCursorFields, null, null);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Cursor cursor = (Cursor) o;
        return Objects.equals(index, cursor.index) && Objects.equals(cursorFields, cursor.cursorFields) && Objects.equals(pitId, cursor.pitId) && Objects.equals(sortValues, cursor.sortValues);
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, cursorFields, pitId, sortValues);
    }
}
