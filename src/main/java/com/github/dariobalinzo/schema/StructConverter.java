package com.github.dariobalinzo.schema;

import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;

import java.util.Map;

public interface StructConverter {
    Struct convert(Map<String, Object> doc, Schema schema);
}
