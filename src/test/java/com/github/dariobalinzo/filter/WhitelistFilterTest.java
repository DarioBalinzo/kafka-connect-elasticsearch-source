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

public class WhitelistFilterTest {
    private static final boolean IS_WINDOWS = System.getProperty( "os.name" ).contains( "indow" );
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
        WhitelistFilter whitelistFilter = new WhitelistFilter(filterValues);
        whitelistFilter.filter(elasticDocument);

        //then
        Assert.assertEquals("{name=elastic, surname=search, version=7}", elasticDocument.toString());
    }

    @SuppressWarnings("unchecked")
    @Test
    public void shouldConvertNestedDocument() throws IOException {
        //given
        String file = this.getClass().getClassLoader()
                .getResource("com/github/dariobalinzo/filter/document.json")
                .getFile();
        String osAppropriateFilePath = IS_WINDOWS ? file.substring(1) : file;
        String jsonDocument = new String(Files.readAllBytes(Paths.get(osAppropriateFilePath)));

        Map<String, Object> elasticDocument = objectMapper.readValue(jsonDocument, Map.class);

        //when
        Set<String> whitelist = Stream.of(
                "name",
                "obj.details.qty",
                "order_list.details.qty"
        ).collect(Collectors.toSet());
        WhitelistFilter whitelistFilter = new WhitelistFilter(whitelist);
        whitelistFilter.filter(elasticDocument);

        //then
        assertEquals(
                "{name=elastic, order_list=[{details={qty=1}}, {details={qty=2}}], obj={details={qty=2}}}",
                elasticDocument.toString());
    }

}
