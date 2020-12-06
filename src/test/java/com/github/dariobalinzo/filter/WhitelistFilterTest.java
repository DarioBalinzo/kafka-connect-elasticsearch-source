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


import com.github.dariobalinzo.schema.SchemaConverter;
import com.github.dariobalinzo.schema.StructConverter;
import org.junit.Test;

import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class WhitelistFilterTest {

    private final SchemaConverter schemaConverter = new SchemaConverter();
    private final StructConverter structConverter = new StructConverter();

    @Test
    public void shouldConvertSimpleSchema() {
        //given
        Map<String, Object> elasticDocument = new LinkedHashMap<>();  //this is a FIFO queue, the order is maintained
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
        WhitelistFilter whitelistFilter = new WhitelistFilter(filterValues);
        Map<String, Object> filteredElasticDocument = whitelistFilter.filter(elasticDocument);

        //then
        assert (filteredElasticDocument.toString()).equals("{surname=search, name=elastic, version=7}");
    }

    private static class NotSupported {
    }
}
