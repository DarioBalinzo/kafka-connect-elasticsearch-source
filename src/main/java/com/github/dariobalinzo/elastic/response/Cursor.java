package com.github.dariobalinzo.elastic.response;

public class Cursor {
    private final String primaryCursor;
    private final String secondaryCursor;

    public Cursor(String primaryCursor, String secondaryCursor) {
        this.primaryCursor = primaryCursor;
        this.secondaryCursor = secondaryCursor;
    }

    public Cursor(String primaryCursor) {
        this.primaryCursor = primaryCursor;
        this.secondaryCursor = null;
    }

    public String getPrimaryCursor() {
        return primaryCursor;
    }

    public String getSecondaryCursor() {
        return secondaryCursor;
    }

    public static Cursor empty() {
        return new Cursor(null, null);
    }

    @Override
    public String toString() {
        return "Cursor{" +
                "primaryCursor='" + primaryCursor + '\'' +
                ", secondaryCursor='" + secondaryCursor + '\'' +
                '}';
    }
}
