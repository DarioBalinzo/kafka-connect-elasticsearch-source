package com.github.dariobalinzo.schema;

import org.apache.kafka.connect.data.Schema;

import java.util.Map;

public interface SchemaConverter {
    Schema convert(Map<String, Object> elasticDocument, String schemaName);
}
