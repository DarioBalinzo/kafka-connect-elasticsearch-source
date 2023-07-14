package com.github.dariobalinzo.elastic.response;

import java.beans.Transient;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Objects;


public record Cursor(String index, List<CursorField> cursorFields, String pitId, Object[] sortValues,
                     int runningDocumentCount, long scrollLimit) {

    public Cursor {
        Objects.requireNonNull(index);
        Objects.requireNonNull(cursorFields);
        cursorFields = Collections.unmodifiableList(cursorFields);
    }


    @Transient
    public boolean isScrollable() {
        return pitId != null;
    }


    public static Cursor of(String index, List<CursorField> cursorFields) {
        return new Cursor(index, cursorFields, null, null, 0, 0);
    }


    public Cursor scrollable(String pitId, Object[] sortValues, int documentCount, long scrollLimit) {
        return new Cursor(this.index(), this.cursorFields, pitId, sortValues, this.runningDocumentCount + documentCount,
            scrollLimit);
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
        return ("Cursor{index='%s', cursorFields=%s, pitId='%s', sortValues=%s, "
            + "runningDocumentCount=%d, scrollLimit=%d}").formatted(index, cursorFields, pitId,
            Arrays.toString(sortValues), runningDocumentCount, scrollLimit);
    }

    public Cursor reframe(Object[] sortValues) {
        return reframe(sortValues, false, false);
    }

    public Cursor reframe(Object[] sortValues, boolean includeLower, boolean incrementFailureCount) {
        final List<CursorField> newCursorFields;
        if (sortValues == null || sortValues.length == 0) {
            newCursorFields = cursorFields;
        } else {
            newCursorFields = new ArrayList<>(cursorFields.size());

            for (int i = 0; i < cursorFields.size(); i++) {
                newCursorFields.add(new CursorField(cursorFields.get(i).field(), sortValues[i]));
            }
        }

        return new Cursor(this.index, newCursorFields, null, null, 0, 0);
    }
}
