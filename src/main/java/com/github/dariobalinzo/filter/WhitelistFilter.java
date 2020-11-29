package com.github.dariobalinzo.filter;

import java.util.List;
import java.util.Map;
import java.util.stream.Collector;
import java.util.stream.Collectors;

public class WhitelistFilter implements Filter {

    List<String> allowedValues;

    public WhitelistFilter(List<String> allowedValues) {
        this.allowedValues = allowedValues;
    }

    public List<String> getAllowedValues() {
        return allowedValues;
    }

    public void setAllowedValues(List<String> allowedValues) {
        this.allowedValues = allowedValues;
    }

    @Override
    public Map<String, Object> filter(Map<String, Object> document) {
        return document.entrySet().stream()
                .filter(key -> allowedValues.contains(key.getKey()))
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }
}
