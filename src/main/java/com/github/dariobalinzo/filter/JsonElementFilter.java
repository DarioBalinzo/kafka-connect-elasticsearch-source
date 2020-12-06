package com.github.dariobalinzo.filter;

@FunctionalInterface
public interface JsonElementFilter {
    Object filterElement(String fieldPath, Object value);
}
