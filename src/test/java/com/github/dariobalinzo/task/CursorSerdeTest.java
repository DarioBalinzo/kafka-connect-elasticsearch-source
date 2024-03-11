package com.github.dariobalinzo.task;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.dariobalinzo.elastic.response.Cursor;
import com.github.dariobalinzo.elastic.response.CursorField;
import com.github.dariobalinzo.task.OffsetSerializer.CursorSerde;
import junit.framework.TestCase;

import java.util.ArrayList;
import java.util.List;

public class CursorSerdeTest extends TestCase {

    public void testCanSerializeInitialCursor() throws Exception {
        final Cursor cursor = Cursor.of("some_index",
            List.of(new CursorField("firstField", Long.MAX_VALUE), new CursorField("secondField", "")));
        final CursorSerde serde = new OffsetSerializer.CursorSerde(new ObjectMapper());

        // serialize
        final String serialized = serde.serialize(cursor);
        assertEquals(
            "{\"index\":\"some_index\",\"cursorFields\":[{\"field\":\"firstField\",\"initialValue\":9223372036854775807},{\"field\":\"secondField\",\"initialValue\":\"\"}],\"pitId\":null,\"sortValues\":null}",
            serialized);

        // deserialize
        final Cursor deserialized = serde.deserialize(serialized);
        assertEquals(cursor, deserialized);
    }

    public void testCanSerializeIntermediateCursor() throws Exception {
        final Cursor cursor = new Cursor("some_index",
            List.of(new CursorField("firstField", Long.MAX_VALUE), new CursorField("secondField", "")), "some_pit_id", new ArrayList());
        final CursorSerde serde = new OffsetSerializer.CursorSerde(new ObjectMapper());

        // serialize
        final String serialized = serde.serialize(cursor);
        assertEquals(
            "{\"index\":\"some_index\",\"cursorFields\":[{\"field\":\"firstField\",\"initialValue\":9223372036854775807},{\"field\":\"secondField\",\"initialValue\":\"\"}],\"pitId\":\"some_pit_id\",\"sortValues\":[]}",
            serialized);

        // deserialize
        final Cursor deserialized = serde.deserialize(serialized);
        assertEquals("Deserialization failed", cursor, deserialized);
    }
}
