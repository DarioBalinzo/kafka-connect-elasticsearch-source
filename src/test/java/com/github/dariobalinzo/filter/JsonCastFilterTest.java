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


import org.junit.Assert;
import org.junit.Test;

import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static junit.framework.TestCase.assertEquals;

public class JsonCastFilterTest {

    @SuppressWarnings("unchecked")
    @Test
    public void shouldConvertSimpleDocument() {
        //given
        Map<String, Object> nestedDoc1 = new LinkedHashMap<>();
        nestedDoc1.put("price", 1);
        nestedDoc1.put("value", "first_element");

        Map<String, Object> nestedDoc2 = new LinkedHashMap<>();
        nestedDoc2.put("price", 2);
        nestedDoc2.put("value", "second_element");

        List<?> nestedList = Collections.singletonList(nestedDoc1);

        Map<String, Object> elasticDocument = new LinkedHashMap<>();
        elasticDocument.put("name", "elastic");
        elasticDocument.put("surname", "search");
        elasticDocument.put("version", 7);
        elasticDocument.put("order_list", nestedList);
        elasticDocument.put("order", nestedDoc2);

        //when
        Set<String> toCast = Stream.of(
                "name",
                "order",
                "order_list"
        ).collect(Collectors.toSet());
        JsonCastFilter jsonCastFilter = new JsonCastFilter(toCast);
        jsonCastFilter.filter(elasticDocument);

        //then
        assertEquals(5, elasticDocument.keySet().size());
        assertEquals( "\"elastic\"", elasticDocument.get("name"));
        assertEquals( "{\"price\":2,\"value\":\"second_element\"}", elasticDocument.get("order"));
        assertEquals( "[{\"price\":1,\"value\":\"first_element\"}]", elasticDocument.get("order_list"));
    }

}
