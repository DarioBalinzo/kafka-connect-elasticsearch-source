package com.github.dariobalinzo.schema;

public class NopNameConverter implements FieldNameConverter {

    public String from(String elasticName) {
        return elasticName;
    }

    public String from(String prefix, String elasticName) {
        return elasticName == null ? prefix : prefix + elasticName;
    }

}
