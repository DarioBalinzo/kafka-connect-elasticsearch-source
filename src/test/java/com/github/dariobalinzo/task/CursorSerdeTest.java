package com.github.dariobalinzo.task;

import co.elastic.clients.elasticsearch._types.FieldValue;
import co.elastic.clients.json.JsonData;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dariobalinzo.TestContainersContext;
import com.github.dariobalinzo.elastic.ElasticRepository;
import com.github.dariobalinzo.elastic.response.Cursor;
import com.github.dariobalinzo.elastic.response.CursorField;
import com.github.dariobalinzo.task.OffsetSerializer.CursorSerde;
import junit.framework.TestCase;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

public class CursorSerdeTest {

    @Test
    public void testCanSerializeInitialCursor() throws Exception {
        final Cursor cursor = Cursor.of("some_index",
            List.of(new CursorField("firstField", Long.MAX_VALUE), new CursorField("secondField", "")));
        final CursorSerde serde = new OffsetSerializer.CursorSerde(new ObjectMapper());

        // serialize
        final String serialized = serde.serialize(cursor);
        assertEquals(
            "{\"index\":\"some_index\",\"cursorFields\":[{\"field\":\"firstField\",\"initialValue\":9223372036854775807},{\"field\":\"secondField\",\"initialValue\":\"\"}],\"pitId\":null,\"includeLower\":true,\"sortValues\":null}",
            serialized);

        // deserialize
        final Cursor deserialized = serde.deserialize(serialized);
        assertEquals(cursor, deserialized);
    }

    @Test
    public void testCanSerializeIntermediateCursor() throws Exception {
        final Cursor cursor = new Cursor("some_index",
            List.of(
                new CursorField("firstField", Long.MAX_VALUE),
                new CursorField("secondField", "")
            ),
            "some_pit_id",
            List.of(FieldValue.of(1L), FieldValue.of("another_sort_value")));
        final CursorSerde serde = new OffsetSerializer.CursorSerde(new ObjectMapper());

        // serialize
        final String serialized = serde.serialize(cursor);
        assertEquals(
            "{\"index\":\"some_index\",\"cursorFields\":[{\"field\":\"firstField\",\"initialValue\":9223372036854775807},{\"field\":\"secondField\",\"initialValue\":\"\"}],\"pitId\":\"some_pit_id\",\"includeLower\":true,\"sortValues\":[{\"l\":\"Long\",\"r\":1},{\"l\":\"String\",\"r\":\"another_sort_value\"}]}",
            serialized);

        // deserialize
        final Cursor deserialized = serde.deserialize(serialized);
        assertEquals("Deserialization failed", cursor, deserialized);
    }
}
