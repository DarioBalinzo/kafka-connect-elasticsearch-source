package com.github.dariobalinzo;

import com.github.dariobalinzo.schema.AvroName;
import com.github.dariobalinzo.schema.FieldNameConverter;
import com.github.dariobalinzo.schema.NopNameConverter;
import com.github.dariobalinzo.schema.SchemaConverter;
import com.github.dariobalinzo.schema.StructConverter;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.junit.Before;
import org.junit.Test;
import org.testcontainers.shaded.com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

public class FooTest {

    private SchemaConverter schemaConverter;
    private StructConverter structConverter;

    Map<String, Object> elasticDocument;

    @Before
    public void setup() throws IOException {
        FieldNameConverter fieldNameConverter = new NopNameConverter();
        this.schemaConverter = new SchemaConverter(fieldNameConverter);
        this.structConverter = new StructConverter(fieldNameConverter);

        String doc = new String(Files.readAllBytes(Paths.get("src/test/java/com/github/dariobalinzo/foo.json")));
        elasticDocument = new ObjectMapper().readValue(doc, Map.class);
    }

    @Test
    public void foo() {

        Schema schema = schemaConverter.convert(elasticDocument, "foo");
        Struct struct = structConverter.convert(elasticDocument, schema);

        System.out.println(struct);
    }
}
