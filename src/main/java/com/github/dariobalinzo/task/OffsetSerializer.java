package com.github.dariobalinzo.task;

import com.github.dariobalinzo.elastic.CursorField;

import java.util.HashMap;
import java.util.Map;

import static com.github.dariobalinzo.task.ElasticSourceTask.POSITION;
import static com.github.dariobalinzo.task.ElasticSourceTask.POSITION_SECONDARY;

public class OffsetSerializer {

    public Map<String, String> toMapOffset(CursorField primaryCursor, CursorField secondaryCursor, Map<String, Object> document) {
        Map<String, String> result = new HashMap<>();
        result.put(POSITION, primaryCursor.read(document));
        if (secondaryCursor != null) {
            result.put(POSITION_SECONDARY, secondaryCursor.read(document));
        }
        return result;
    }

    public String toStringOffset(CursorField cursor, CursorField secondaryCursor, String index, Map<String, Object> document) {
        String cursorValue = cursor.read(document);
        if (secondaryCursor == null) {
            return String.join("_", index, cursorValue);
        } else {
            return String.join("_", index, cursorValue, secondaryCursor.read(document));
        }
    }
}
