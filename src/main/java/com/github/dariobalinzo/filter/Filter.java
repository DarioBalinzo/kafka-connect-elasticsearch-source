package com.github.dariobalinzo.filter;

import java.util.Map;

public interface Filter {

    public Map<String, Object> filter(Map<String, Object> document);

}
