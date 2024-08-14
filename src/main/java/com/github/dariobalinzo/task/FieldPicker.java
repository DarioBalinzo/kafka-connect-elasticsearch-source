package com.github.dariobalinzo.task;

import com.github.dariobalinzo.elastic.CursorField;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.github.dariobalinzo.task.ElasticSourceTask.POSITION;
import static com.github.dariobalinzo.task.ElasticSourceTask.POSITION_SECONDARY;

public class FieldPicker {
    public Map<String, String> pick(Map<String, Object> document, Map<String, CursorField> fields) {
        Map<String, String> result = new HashMap<>();
        for (Map.Entry<String, CursorField> entry : fields.entrySet()) {
            String readValue = entry.getValue().read(document);
            if (readValue == null)
                continue;
            result.put(entry.getKey(), readValue);
        }
        return result;
    }

    public Map<String, Object> pick(Map<String, Object> document, String ...fields) {
        Map<String, Object> result = new HashMap<String, Object>();
        pick(result, document, fields);
        return result;
    }

    private void pick(Map<String, Object> result, Map<String, Object> document, String ...fields) {
        Map<Map<String, Object>, List<String>> nextLoops = new HashMap<>();
        for (String field : fields) {
            if (field.length() == 0) {
                continue;
            }
            if (document.containsKey(field)) {
                result.put(field, document.get(field));
                continue;
            }
            if (field.length() < 3) {
                continue;
            }
            if (field.indexOf(".") < 1) {
                continue;
            }
            String firstField = field.substring(0, field.indexOf("."));
            if (!document.containsKey(firstField)) {
                continue;
            }
            Object value = document.get(firstField);
            if (!(value instanceof Map)) {
                continue;
            }
            Map<String, Object> structValue = (Map<String, Object>)value;
            List<String> nFields = nextLoops.get(structValue);
            String nField = field.substring(field.indexOf(".") + 1);
            if (nFields != null) {
                nFields.add(nField);
                continue;
            }
            nextLoops.put(structValue, new ArrayList<String>(Arrays.asList(nField)));
        }
        for (Map.Entry<Map<String, Object>, List<String>> nextLoop : nextLoops.entrySet()) {
            pick(
                result,
                nextLoop.getKey(),
                nextLoop.getValue().toArray(new String[nextLoop.getValue().size()])
            );
        }
    }
}