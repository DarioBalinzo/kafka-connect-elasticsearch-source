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
import org.junit.Assert;
import org.junit.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static junit.framework.TestCase.assertEquals;

public class BlacklistFilterTest {
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    public void shouldConvertSimpleSchema() {
        //given
        Map<String, Object> elasticDocument = new LinkedHashMap<>();
        elasticDocument.put("name", "elastic");
        elasticDocument.put("surname", "search");
        elasticDocument.put("version", 7);
        elasticDocument.put("enabled", true);

        //when
        Set<String> filterValues = Stream.of(
                "name",
                "surname",
                "version"
        ).collect(Collectors.toCollection(HashSet::new));
        BlacklistFilter BlacklistFilter = new BlacklistFilter(filterValues);
        BlacklistFilter.filter(elasticDocument);

        //then
        Assert.assertEquals("{enabled=true}", elasticDocument.toString());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldConvertNestedDocument() throws IOException {
        //given
        String file = this.getClass().getClassLoader()
                .getResource("com/github/dariobalinzo/filter/document.json")
                .getFile();
        String jsonDocument = new String(Files.readAllBytes(Paths.get(file)));

        Map<String, Object> elasticDocument = objectMapper.readValue(jsonDocument, Map.class);

        //when
        Set<String> whitelist = Stream.of(
                "name",
                "obj.details.qty",
                "order_list.details.qty"
        ).collect(Collectors.toSet());
        BlacklistFilter BlacklistFilter = new BlacklistFilter(whitelist);
        BlacklistFilter.filter(elasticDocument);

        //then
        assertEquals(
                "{age=7, order_list=[{id=1, details={nested_det=test nested inside list}}, {id=2, details={nested_det=test nested inside list}}], obj={key=55, details={nested_det=test nested inside list}}}",
                elasticDocument.toString());
    }

}
