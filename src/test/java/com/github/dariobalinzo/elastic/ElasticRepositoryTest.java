/**
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

package com.github.dariobalinzo.elastic;

import com.github.dariobalinzo.TestContainersContext;
import com.github.dariobalinzo.elastic.response.Cursor;
import com.github.dariobalinzo.elastic.response.PageResult;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.*;

public class ElasticRepositoryTest extends TestContainersContext {


    @Test
    public void shouldFetchDataFromElastic() throws IOException, InterruptedException {
        deleteTestIndex();

        insertMockData(111);
        insertMockData(112);
        insertMockData(113);
        insertMockData(114);
        refreshIndex();

        PageResult firstPage = repository.searchAfter(TEST_INDEX, Cursor.empty());
        assertEquals(3, firstPage.getDocuments().size());

        PageResult secondPage = repository.searchAfter(TEST_INDEX, firstPage.getLastCursor());
        assertEquals(1, secondPage.getDocuments().size());

        PageResult emptyPage = repository.searchAfter(TEST_INDEX, secondPage.getLastCursor());
        assertEquals(0, emptyPage.getDocuments().size());
        assertNull(emptyPage.getLastCursor().getPrimaryCursor());

        assertEquals(Collections.singletonList(TEST_INDEX), repository.catIndices("source"));
        assertEquals(Collections.emptyList(), repository.catIndices("non-existing"));
    }

    @Test
    public void shouldListExistingIndices() throws IOException, InterruptedException {
        deleteTestIndex();
        insertMockData(111);
        refreshIndex();

        assertEquals(Collections.singletonList(TEST_INDEX), repository.catIndices("source"));
        assertEquals(Collections.emptyList(), repository.catIndices("non-existing"));
    }

    @Test
    public void shouldFetchDataUsingSecondarySortField() throws IOException, InterruptedException {
        deleteTestIndex();

        insertMockData(111, "customerA", TEST_INDEX);
        insertMockData(111, "customerB", TEST_INDEX);
        insertMockData(111, "customerC", TEST_INDEX);
        insertMockData(111, "customerD", TEST_INDEX);
        insertMockData(112, "customerA", TEST_INDEX);
        insertMockData(113, "customerB", TEST_INDEX);
        insertMockData(113, "customerC", TEST_INDEX);
        insertMockData(113, "customerD", TEST_INDEX);

        refreshIndex();

        PageResult firstPage = secondarySortRepo.searchAfterWithSecondarySort(TEST_INDEX, Cursor.empty());
        assertEquals(3, firstPage.getDocuments().size());

        PageResult secondPage = secondarySortRepo.searchAfterWithSecondarySort(TEST_INDEX, firstPage.getLastCursor());
        assertEquals(3, secondPage.getDocuments().size());

        PageResult thirdPage = secondarySortRepo.searchAfterWithSecondarySort(TEST_INDEX, secondPage.getLastCursor());
        assertEquals(2, thirdPage.getDocuments().size());

        PageResult emptyPage = secondarySortRepo.searchAfterWithSecondarySort(TEST_INDEX, thirdPage.getLastCursor());
        assertEquals(0, emptyPage.getDocuments().size());
        assertNull(emptyPage.getLastCursor().getPrimaryCursor());
        assertNull(emptyPage.getLastCursor().getSecondaryCursor());
    }

    @Test
    public void shouldFetchDataWithAdditionalField() throws IOException, InterruptedException {
        deleteTestIndex();

        insertMockData(110, "customerA", TEST_INDEX);
        insertMockData(111, "customerB", TEST_INDEX);
        refreshIndex();

        PageResult firstPage = repository.searchAfter(TEST_INDEX, Cursor.empty());
        firstPage.getDocuments().forEach(item -> {
            assertNotNull(item.get((String) "es-index"));
            assertNotNull(item.get((String) "es-id"));
        });
    }

}
