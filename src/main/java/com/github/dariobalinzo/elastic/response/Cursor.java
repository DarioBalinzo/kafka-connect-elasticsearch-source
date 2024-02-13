package com.github.dariobalinzo.elastic.response;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.beans.Transient;
import java.util.*;


public class Cursor {

    private final String index;
    private final List<CursorField> cursorFields;
    private final String pitId;
    private final Object[] sortValues;
    private final int runningDocumentCount;
    private final long scrollLimit;


    @JsonCreator(mode = JsonCreator.Mode.PROPERTIES)
    public Cursor(@JsonProperty("index") String index, @JsonProperty("cursorFields") List<CursorField> cursorFields, @JsonProperty("pitId") String pitId,
        @JsonProperty("sortValues") Object[] sortValues, @JsonProperty("runningDocumentCount") int runningDocumentCount, @JsonProperty("scrollLimit") long scrollLimit) {
        Objects.requireNonNull(index);
        Objects.requireNonNull(cursorFields);
        cursorFields = Collections.unmodifiableList(cursorFields);
        this.index = index;
        this.cursorFields = cursorFields;
        this.pitId = pitId;
        this.sortValues = sortValues;
        this.runningDocumentCount = runningDocumentCount;
        this.scrollLimit = scrollLimit;
    }


    @Transient
    public boolean isScrollable() {
        return pitId != null;
    }


    public static Cursor of(String index, List<CursorField> cursorFields) {
        return new Cursor(index, cursorFields, null, null, 0, 0);
    }


    public Cursor scrollable(String pitId, Object[] sortValues, int documentCount, long scrollLimit) {
        return new Cursor(this.getIndex(), this.cursorFields, pitId, sortValues,
            this.runningDocumentCount + documentCount, scrollLimit);
    }

    @Override
    // Overriding equals and hashCode for the sortValues object array which falls back to the Object default
    // where the instances must be the same to be equal.
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        Cursor cursor = (Cursor) o;
        return Objects.equals(index, cursor.index) && Objects.equals(cursorFields, cursor.cursorFields)
            && Objects.equals(pitId, cursor.pitId) && Arrays.equals(sortValues, cursor.sortValues) && Objects.equals(
            runningDocumentCount, cursor.runningDocumentCount);
    }


    @Override
    public int hashCode() {
        int result = Objects.hash(index, cursorFields, pitId, runningDocumentCount);
        result = 31 * result + Arrays.hashCode(sortValues);
        return result;
    }

    @Override
    public String toString() {
        return "Cursor{" +
                "index='" + index + '\'' +
                ", cursorFields=" + cursorFields +
                ", pitId='" + pitId + '\'' +
                ", sortValues=" + Arrays.toString(sortValues) +
                ", runningDocumentCount=" + runningDocumentCount +
                ", scrollLimit=" + scrollLimit +
                '}';
    }

    public Cursor reframe(Object[] sortValues) {
        final List<CursorField> newCursorFields;
        if (sortValues == null || sortValues.length == 0) {
            newCursorFields = cursorFields;
        } else {
            newCursorFields = new ArrayList<>(cursorFields.size());

            for (int i = 0; i < cursorFields.size(); i++) {
                newCursorFields.add(new CursorField(cursorFields.get(i).getField(), sortValues[i]));
            }
        }

        return new Cursor(this.index, newCursorFields, null, null, 0, 0);
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

    public Object[] getSortValues() {
        return sortValues;
    }

    public int getRunningDocumentCount() {
        return runningDocumentCount;
    }

    public long getScrollLimit() {
        return scrollLimit;
    }


    public Map<String, Object> toMap() {
        Map<String, Object> map = new HashMap<>();
        map.put("index", index);
        map.put("cursorFields", cursorFields);
        map.put("pitId", pitId);
        map.put("sortValues", sortValues);
        map.put("runningDocumentCount", runningDocumentCount);
        map.put("scrollLimit", scrollLimit);
        return map;
    }

    public static Cursor fromMap(Map<String, Object> map) {
        return new Cursor((String) map.get("index"),
                (List<CursorField>) map.get("cursorFields"),
                (String) map.get("pitId"),
                (Object[]) map.get("sortValues"),
                (int) map.get("runningDocumentCount"),
                (long) map.get("scrollLimit"));
    }
}
