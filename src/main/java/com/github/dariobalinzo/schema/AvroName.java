package com.github.dariobalinzo.schema;

public class AvroName {

    public static String from(String elasticName) {
        return elasticName == null ? null : filterInvalidCharacters(elasticName);
    }

    public static String from(String prefix, String elasticName) {
        return elasticName == null ? prefix : prefix + filterInvalidCharacters(elasticName);
    }

    private static String filterInvalidCharacters(String elasticName) {
        return elasticName.replaceAll("[^a-zA-Z0-9]", "");
    }
}
