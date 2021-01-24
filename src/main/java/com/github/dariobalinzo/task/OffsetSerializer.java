package com.github.dariobalinzo.task;

import java.util.HashMap;
import java.util.Map;

import static com.github.dariobalinzo.task.ElasticSourceTask.POSITION;
import static com.github.dariobalinzo.task.ElasticSourceTask.POSITION_SECONDARY;

public class OffsetSerializer {

    public Map<String, String> toMapOffset(String primaryCursor, String secondaryCursor, Map<String, Object> document) {
        Map<String, String> result = new HashMap<>();
        result.put(POSITION, document.get(primaryCursor).toString());
        if (secondaryCursor != null) {
            result.put(POSITION_SECONDARY, document.get(secondaryCursor).toString());
        }
        return result;
    }

    public String toStringOffset(String cursor, String secondaryCursor, String index, Map<String, Object> document) {
        String cursorValue = document.get(cursor).toString();
        if (secondaryCursor == null) {
            return String.join("_", index, cursorValue);
        } else {
            return String.join("_", index, cursorValue, document.get(secondaryCursor).toString());
        }
    }
}
