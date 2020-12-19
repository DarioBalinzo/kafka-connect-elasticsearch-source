package com.github.dariobalinzo.schema;

public interface FieldNameConverter {

    String from(String elasticName);

    String from(String prefix, String elasticName);

}
