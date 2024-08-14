package com.github.dariobalinzo.task;

import com.github.dariobalinzo.elastic.CursorField;

import java.util.Map;
import java.util.HashMap;
import org.junit.Test;

import static org.junit.Assert.*;

public class FieldPickerTest {
    Map<String, Object> data = new HashMap<String, Object>() {{
        put("a", new HashMap<String, Object>() {{
            put("b", new HashMap<String, Object>() {{
                put("c", new HashMap<String, Object>() {{
                    put("d", new HashMap<String, Object>() {{
                        put("e", "hadouken");
                        put("f", "hadouken-border");
                    }});
                }});
                put("bb", "mama");
                put("b3", "capital");
            }});
        }});
    }};

    @Test
    public void callingPick_withProvidedFields_shouldGetFields() throws Exception {
        //given
        String[] fieldsToPick = new String[] {
            "a.b.c.d.e",
            "a.b.c.d.f",
            "a.b.bb"
        };
        FieldPicker fp = new FieldPicker();

        //when
        Map<String, Object> picked = fp.pick(data, fieldsToPick);

        //then
        assertNotNull(picked);
        assertEquals(picked.entrySet().size(), 3);
        assertTrue(picked.containsKey("e"));
        assertTrue(picked.containsKey("f"));
        assertTrue(picked.containsKey("bb"));
        assertEquals(picked.get("e"), "hadouken");
        assertEquals(picked.get("f"), "hadouken-border");
        assertEquals(picked.get("bb"), "mama");
    }

    @Test
    public void callingPick_withProvidedKeyCursorFieldMap_shouldGetFields() throws Exception {
        //given
        Map<String, CursorField> fieldsToPick = new HashMap<String, CursorField>() {{
            put("something", new CursorField("a.b.c.d.e"));
            put("other-thing", new CursorField("a.b.c.d.f"));
            put("that-thing", new CursorField("a.b.b3"));
        }};
        FieldPicker fp = new FieldPicker();

        //when
        Map<String, String> picked = fp.pick(data, fieldsToPick);

        //then
        assertNotNull(picked);
        assertEquals(picked.entrySet().size(), 3);
        assertTrue(picked.containsKey("something"));
        assertTrue(picked.containsKey("other-thing"));
        assertTrue(picked.containsKey("that-thing"));
        assertEquals(picked.get("something"), "hadouken");
        assertEquals(picked.get("other-thing"), "hadouken-border");
        assertEquals(picked.get("that-thing"), "capital");
    }
}
