package com.github.dariobalinzo.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dariobalinzo.elastic.response.Cursor;
import com.github.dariobalinzo.elastic.response.CursorField;
import junit.framework.TestCase;

import java.util.List;

public class CursorSerdeTest extends TestCase {

    public void testCanSerializeInitialCursor() throws Exception {
        final var cursor = Cursor.of("some_index",
            List.of(new CursorField("firstField", Long.MAX_VALUE), new CursorField("secondField", "")));
        final var serde = new OffsetSerializer.CursorSerde(new ObjectMapper());

        // serialize
        final var serialized = serde.serialize(cursor);
        assertEquals(
            "{\"index\":\"some_index\",\"cursorFields\":[{\"field\":\"firstField\",\"initialValue\":9223372036854775807},{\"field\":\"secondField\",\"initialValue\":\"\"}],\"pitId\":null,\"sortValues\":null,\"runningDocumentCount\":0,\"scrollLimit\":0}",
            serialized);

        // deserialize
        final var deserialized = serde.deserialize(serialized);
        assertEquals(cursor, deserialized);
    }

    public void testCanSerializeIntermediateCursor() throws Exception {
        final var cursor = new Cursor("some_index",
            List.of(new CursorField("firstField", Long.MAX_VALUE), new CursorField("secondField", "")), "some_pit_id", new Object[]{4711, "some_secondary_value", 37}, 53, 64);
        final var serde = new OffsetSerializer.CursorSerde(new ObjectMapper());

        // serialize
        final var serialized = serde.serialize(cursor);
        assertEquals(
            "{\"index\":\"some_index\",\"cursorFields\":[{\"field\":\"firstField\",\"initialValue\":9223372036854775807},{\"field\":\"secondField\",\"initialValue\":\"\"}],\"pitId\":\"some_pit_id\",\"sortValues\":[4711,\"some_secondary_value\",37],\"runningDocumentCount\":53,\"scrollLimit\":64}",
            serialized);

        // deserialize
        final var deserialized = serde.deserialize(serialized);
        assertEquals("Deserialization failed", cursor, deserialized);
    }
}
