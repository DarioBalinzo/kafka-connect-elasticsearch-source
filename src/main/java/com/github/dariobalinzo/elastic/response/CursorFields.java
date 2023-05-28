package com.github.dariobalinzo.elastic.response;

import java.util.Objects;

import static com.github.dariobalinzo.elastic.ElasticJsonNaming.removeKeywordSuffix;

public class CursorFields {
    private final String primaryCursorField;
    private final String secondaryCursorField;

    //a field like customer.keyword is represented as customer inside json response
    private final String primaryCursorFieldJsonName;
    private final String secondaryCursorFieldJsonName;
    private final CursorType cursorType;

    public CursorFields(String primaryCursorField) {
        this(primaryCursorField, null);
    }

    public CursorFields(String primaryCursorField, String secondaryCursorField) {
        Objects.requireNonNull(primaryCursorField);
        this.primaryCursorField = primaryCursorField;
        this.secondaryCursorField = secondaryCursorField;

        primaryCursorFieldJsonName = removeKeywordSuffix(primaryCursorField);
        secondaryCursorFieldJsonName = removeKeywordSuffix(secondaryCursorField);

        cursorType = secondaryCursorField == null ? CursorType.PRIMARY_ONLY : CursorType.PRIMARY_AND_SECONDARY;
    }

    public String getPrimaryCursorField() {
        return primaryCursorField;
    }

    public String getSecondaryCursorField() {
        return secondaryCursorField;
    }

    public String getPrimaryCursorFieldJsonName() {
        return primaryCursorFieldJsonName;
    }

    public String getSecondaryCursorFieldJsonName() {
        return secondaryCursorFieldJsonName;
    }

    @Override
    public String toString() {
        return "CursorFields{" +
                "primaryCursorField='" + primaryCursorField + '\'' +
                ", secondaryCursorField='" + secondaryCursorField + '\'' +
                '}';
    }

    public CursorType getCursorType() {
        return cursorType;
    }

    public static enum CursorType{
        PRIMARY_ONLY, PRIMARY_AND_SECONDARY
    }

    public Cursor newEmptyCursor() {
        return new Cursor(this, null, null);
    }

    public Cursor newCursor(String primary ) {
        return this.newCursor(primary, null);
    }
    public Cursor newCursor(String primary, String secondary ) {
        return new Cursor(this, primary, secondary);
    }


    public static class Cursor {
        private final CursorFields cursorFields;
        private final String primaryCursor;
        private final String secondaryCursor;

        private Cursor(CursorFields cursorFields, String primaryCursor) {
            this(cursorFields, primaryCursor, null);
        }

        private Cursor(CursorFields cursorFields, String primaryCursor, String secondaryCursor) {
            this.cursorFields = cursorFields;
            this.primaryCursor = primaryCursor;
            this.secondaryCursor = secondaryCursor;
        }


        public CursorFields getCursorFields() {
            return cursorFields;
        }

        public String getPrimaryCursor() {
            return primaryCursor;
        }

        public String getSecondaryCursor() {
            return secondaryCursor;
        }

        public boolean isEmpty() {
            return primaryCursor == null;
        }

        public Cursor newEmptyCursor() {
            return cursorFields.newEmptyCursor();
        }

        public Cursor newCursor(String primary) {
            return cursorFields.newCursor(primary);
        }

        public Cursor newCursor(String primary, String secondary) {
            return cursorFields.newCursor(primary, secondary);
        }

        @Override
        public String toString() {
            return "Cursor{" +
                    "primaryCursor='" + primaryCursor + '\'' +
                    ", secondaryCursor='" + secondaryCursor + '\'' +
                    '}';
        }
    }
}