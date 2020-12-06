package com.github.dariobalinzo.filter;

import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class WhitelistFilter implements DocumentFilter {

    private final Set<String> allowedValues;

    public WhitelistFilter(Set<String> allowedValues) {
        this.allowedValues = allowedValues;
    }

    public Set<String> getAllowedValues() {
        return allowedValues;
    }

    @Override
    public Map<String, Object> filter(Map<String, Object> document) {
        return document.entrySet().stream()
                .filter(key -> allowedValues.contains(key.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
