package com.github.dariobalinzo.schema;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.SchemaBuilder;

import java.util.Map;

public class SimpleStringSchemaConverter implements SchemaConverter{
    @Override
    public Schema convert(Map<String, Object> elasticDocument, String schemaName) {
        return SchemaBuilder.struct().field("doc", Schema.STRING_SCHEMA);
    }
}
