/*
 * Copyright © 2018 Dario Balinzo (dariobalinzo@gmail.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.github.dariobalinzo.filter;


import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static junit.framework.TestCase.assertEquals;

public class JsonCastFilterTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @SuppressWarnings("unchecked")
    @Test
    public void shouldConvertSimpleDocument() throws IOException {
        //given
        String file = this.getClass().getClassLoader()
                .getResource("com/github/dariobalinzo/filter/document.json")
                .getFile();
        String jsonDocument = new String(Files.readAllBytes(Paths.get(file)));

        Map<String, Object> elasticDocument = objectMapper.readValue(jsonDocument, Map.class);

        //when
        Set<String> toCast = Stream.of(
                "name",
                "obj.details",
                "order_list.details"
        ).collect(Collectors.toSet());
        JsonCastFilter jsonCastFilter = new JsonCastFilter(toCast);
        jsonCastFilter.filter(elasticDocument);

        //then
        assertEquals(5, elasticDocument.keySet().size());
        assertEquals("\"elastic\"", elasticDocument.get("name"));
        Map<String, Object> obj = (Map<String, Object>) elasticDocument.get("obj");
        assertEquals("{\"nested_det\":\"test nested inside list\",\"qty\":2}", obj.get("details"));

        List<Object> nestedList = (List<Object>) elasticDocument.get("order_list");
        Map<String, Object> nestedInsideList1 = (Map<String, Object>) nestedList.get(0);
        Map<String, Object> nestedInsideList2 = (Map<String, Object>) nestedList.get(1);

        assertEquals("{\"nested_det\":\"test nested inside list\",\"qty\":1}", nestedInsideList1.get("details"));
        assertEquals("{\"nested_det\":\"test nested inside list\",\"qty\":2}", nestedInsideList2.get("details"));
    }

}
