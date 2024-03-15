package com.github.dariobalinzo.elastic.response;

import co.elastic.clients.elasticsearch._types.FieldValue;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.util.DefaultPrettyPrinter;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dariobalinzo.task.OffsetSerializer;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.stream.IntStream;


public class Cursor {

    private final String index;
    private final List<CursorField> cursorFields;
    private final String pitId;

    private final List<FieldValue> sortValues;
    private final boolean includeLower;

    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public Cursor(@JsonProperty("index") String index,
                  @JsonProperty("cursorFields") List<CursorField> cursorFields,
                  @JsonProperty("pitId") String pitId,
                  @JsonProperty("includeLower") boolean includeLower,
                  @JsonProperty("sortValues") List<FieldValue> sortValues) {
        Objects.requireNonNull(index);
        Objects.requireNonNull(cursorFields);
        this.index = index;
        this.cursorFields = cursorFields;
        this.pitId = pitId;
        this.sortValues = sortValues;
        this.includeLower = includeLower;
    }

    public Cursor(String index,
                  List<CursorField> cursorFields,
                  String pitId,
                  List<FieldValue> sortValues) {
        this(index, cursorFields, pitId, true, sortValues);
    }

    public static Cursor of(String index, List<CursorField> cursorFields) {
        return new Cursor(index, cursorFields, null, true, null);
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

    public boolean isIncludeLower() {
        return includeLower;
    }

    public Cursor withPitId(String pitId) {
        return new Cursor(this.index, this.cursorFields, pitId, this.includeLower, this.sortValues);
    }

    public Cursor withSortValues(List<FieldValue> sortValues) {
        return new Cursor(this.index, this.cursorFields, this.pitId, this.includeLower, sortValues);
    }


    /**
     * Re-frames the current cursor into a new one with any sort values transferred to cursor fields as initial values
     * and no pitId.
     *
     * @return a new Cursor instance.
     */
    public Cursor reframe(boolean includeLower) {
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

        return new Cursor(this.index, newCursorFields, null, includeLower, null);
    }


    @Override
    public String toString() {
        return new OffsetSerializer.CursorSerde(
            new ObjectMapper().setDefaultPrettyPrinter(new DefaultPrettyPrinter()))
                .serialize(this);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Cursor cursor = (Cursor) o;
        var result = Objects.equals(index, cursor.index)
            && Objects.equals(cursorFields, cursor.cursorFields)
            && Objects.equals(pitId, cursor.pitId);

        if (result) {
            if (sortValues != null && cursor.sortValues != null
                && sortValues.size() == cursor.sortValues.size()) {
                return IntStream.range(0, sortValues.size())
                    .allMatch(i -> Objects.equals(sortValues.get(i)._kind(), cursor.sortValues.get(i)._kind())
                        && Objects.equals(sortValues.get(i)._toJsonString(), cursor.sortValues.get(i)._toJsonString()));
            }

            return sortValues == null && cursor.sortValues == null;
        }

        return false;
    }

    @Override
    public int hashCode() {
        return Objects.hash(index, cursorFields, pitId, sortValues);
    }
}
