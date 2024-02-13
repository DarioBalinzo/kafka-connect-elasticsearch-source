/**
 * Copyright © 2018 Dario Balinzo (dariobalinzo@gmail.com)
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.github.dariobalinzo.elastic;

import com.github.dariobalinzo.TestContainersContext;
import com.github.dariobalinzo.elastic.response.Cursor;
import com.github.dariobalinzo.elastic.response.CursorField;
import org.elasticsearch.ElasticsearchStatusException;
import org.junit.Test;

import java.io.IOException;
import java.util.List;
import java.util.Optional;

import static org.junit.Assert.*;


public class ElasticRepositoryTest extends TestContainersContext {

    //    @Test
    //    public void shouldFetchDataFromElastic() throws IOException, InterruptedException {
    //        deleteTestIndex();
    //
    //        insertMockData(111);
    //        insertMockData(112);
    //        insertMockData(113);
    //        insertMockData(114);
    //        refreshIndex();
    //
    //
    //        try (ElasticPointInTimeResource pointInTime = repository.newPointTime(TEST_INDEX)) {
    //
    //            PageResult firstPage = repository.searchAfter(TEST_INDEX, new CursorFields(CURSOR_FIELD).newEmptyCursor(), pointInTime.getPitId());
    //            assertEquals(3, firstPage.documents().size());
    //
    //            PageResult secondPage = repository.searchAfter(TEST_INDEX, firstPage.getLastCursor(), pointInTime.getPitId());
    //            assertEquals(1, secondPage.documents().size());
    //
    //            PageResult emptyPage = repository.searchAfter(TEST_INDEX, secondPage.getLastCursor(), pointInTime.getPitId());
    //            assertEquals(0, emptyPage.documents().size());
    //            assertNull(emptyPage.getLastCursor().getPrimaryCursor());
    //
    //            assertEquals(Collections.singletonList(TEST_INDEX), repository.catIndices("source"));
    //            assertEquals(Collections.emptyList(), repository.catIndices("non-existing"));
    //        }
    //    }

    //    @Test
    //    public void shouldListExistingIndices() throws IOException, InterruptedException {
    //        deleteTestIndex();
    //        insertMockData(111);
    //        refreshIndex();
    //
    //        assertEquals(Collections.singletonList(TEST_INDEX), repository.catIndices("source"));
    //        assertEquals(Collections.emptyList(), repository.catIndices("non-existing"));
    //    }
    //
    //    @Test
    //    public void shouldFetchDataUsingSecondarySortField() throws IOException, InterruptedException {
    //        deleteTestIndex();
    //
    //        insertMockData(111, "customerA", TEST_INDEX);
    //        insertMockData(111, "customerB", TEST_INDEX);
    //        insertMockData(111, "customerC", TEST_INDEX);
    //        insertMockData(111, "customerD", TEST_INDEX);
    //        insertMockData(112, "customerA", TEST_INDEX);
    //        insertMockData(113, "customerB", TEST_INDEX);
    //        insertMockData(113, "customerC", TEST_INDEX);
    //        insertMockData(113, "customerD", TEST_INDEX);
    //
    //        refreshIndex();
    //
    //        PageResult firstPage = secondarySortRepo.searchAfterWithSecondarySort(TEST_INDEX, new CursorFields(CURSOR_FIELD, SECONDARY_CURSOR_FIELD).newEmptyCursor());
    //        assertEquals(3, firstPage.documents().size());
    //
    //        PageResult secondPage = secondarySortRepo.searchAfterWithSecondarySort(TEST_INDEX, firstPage.getLastCursor());
    //        assertEquals(3, secondPage.documents().size());
    //
    //        PageResult thirdPage = secondarySortRepo.searchAfterWithSecondarySort(TEST_INDEX, secondPage.getLastCursor());
    //        assertEquals(2, thirdPage.documents().size());
    //
    //        PageResult emptyPage = secondarySortRepo.searchAfterWithSecondarySort(TEST_INDEX, thirdPage.getLastCursor());
    //        assertEquals(0, emptyPage.documents().size());
    //        assertNull(emptyPage.getLastCursor().getPrimaryCursor());
    //        assertNull(emptyPage.getLastCursor().getSecondaryCursor());
    //    }
    //
    //    @Test
    //    public void shouldFetchDataWithAdditionalField() throws IOException, InterruptedException {
    //        deleteTestIndex();
    //
    //        insertMockData(110, "customerA", TEST_INDEX);
    //        insertMockData(111, "customerB", TEST_INDEX);
    //        refreshIndex();
    //
    //        PageResult firstPage = repository.searchAfter(TEST_INDEX, new CursorFields(CURSOR_FIELD).newEmptyCursor());
    //        firstPage.documents().forEach(item -> {
    //            assertNotNull(item.get("es-index"));
    //            assertNotNull(item.get("es-id"));
    //        });
    //    }
    //
    //    @Test
    //    public void shouldNotSkipDuplicatesAtBatchBoundaryWithSecondary() throws IOException, InterruptedException {
    //        deleteTestIndex();
    //
    //        insertMockData(111, "customerA", TEST_INDEX);
    //        insertMockData(111, "customerB", TEST_INDEX);
    //        insertMockData(111, "customerC", TEST_INDEX);
    //        insertMockData(111, "customerD", TEST_INDEX);
    //        insertMockData(112, "customerA", TEST_INDEX);
    //        insertMockData(113, "customerB", TEST_INDEX);
    //        insertMockData(113, "customerC", TEST_INDEX);
    //        insertMockData(113, "customerD", TEST_INDEX);
    //        refreshIndex();
    //
    //        ElasticRepository thisRepository = new ElasticRepository(connection);
    //        thisRepository.setPageSize(2);
    //        final var initialCursor = Cursor.of(TEST_INDEX, List.of(
    //                new CursorField(CURSOR_FIELD, 0L),
    //                new CursorField(SECONDARY_CURSOR_FIELD, "")));
    //
    //        var page = thisRepository.search(initialCursor);
    //        assertFalse(page.lastPage());
    //        assertEquals(2, page.documents().size());
    //        assertEquals(111, page.documents().get(0).get("ts"));
    //        assertEquals(111, page.documents().get(1).get("ts"));
    //        assertEquals("customerA", page.documents().get(0).get("fullName"));
    //        assertEquals("customerB", page.documents().get(1).get("fullName"));
    //
    //        page = page.cursor().map(thisRepository::search).orElse(PageResult.empty());
    //        assertEquals(2, page.documents().size());
    //        assertEquals(111, page.documents().get(0).get("ts"));
    //        assertEquals(111, page.documents().get(1).get("ts"));
    //        assertEquals("customerC", page.documents().get(0).get("fullName"));
    //        assertEquals("customerD", page.documents().get(1).get("fullName"));
    //    }


    @Test
    public void shouldNotSkipDuplicatesAtBatchBoundary() throws IOException, InterruptedException {
        deleteTestIndex();

        insertMockData(111);
        insertMockData(112);
        insertMockData(113);
        insertMockData(113);
        insertMockData(113);
        insertMockData(113);
        insertMockData(114);
        refreshIndex();

        ElasticRepository thisRepository = new ElasticRepository(connection);
        thisRepository.setPageSize(2);
        final var initialCursor = Cursor.of(TEST_INDEX, List.of(new CursorField(CURSOR_FIELD, 0L)));

        var page = thisRepository.search(initialCursor);

        assertEquals(2, page.documents().size());
        assertEquals(111, page.documents().get(0).get("ts"));
        assertEquals(112, page.documents().get(1).get("ts"));

        page = Optional.ofNullable(page.cursor()).map(thisRepository::search).orElseThrow();

        assertEquals(2, page.documents().size());
        assertEquals(113, page.documents().get(0).get("ts"));
        assertEquals(113, page.documents().get(1).get("ts"));

        page = Optional.ofNullable(page.cursor()).map(thisRepository::search).orElseThrow();

        assertEquals(2, page.documents().size());
        assertEquals(113, page.documents().get(0).get("ts"));
        assertEquals(113, page.documents().get(1).get("ts"));

        page = Optional.ofNullable(page.cursor()).map(thisRepository::search).orElseThrow();
        assertTrue(page.lastPage());
        assertEquals(1, page.documents().size());
        assertEquals(114, page.documents().get(0).get("ts"));

        //        page = Optional.ofNullable(page.cursor()).map(thisRepository::search).orElseThrow();
        //        assertTrue(page.isEmpty());

        deleteTestIndex();
        insertMockData(115);
        insertMockData(116);
        insertMockData(116);
        refreshIndex();

        page = Optional.ofNullable(page.cursor()).map(thisRepository::search).orElseThrow();
        assertEquals(2, page.documents().size());
    }

    @Test
    public void testGracefulHandlingOfUnexpectedlyClosedPitId() throws IOException, InterruptedException {
        deleteTestIndex();

        insertMockData(111);
        insertMockData(112);
        insertMockData(112);
        insertMockData(112);
        refreshIndex();

        final ElasticRepository thisRepository = new ElasticRepository(connection);
        thisRepository.setPageSize(2);
        final var initialCursor = Cursor.of(TEST_INDEX, List.of(new CursorField(CURSOR_FIELD, 0L)));

        var page = thisRepository.search(initialCursor);
        assertFalse(page.lastPage());

        // close the pit
        Optional.ofNullable(page.cursor()).ifPresent(cursor -> thisRepository.closePit(cursor.getPitId()));

        // should still work - it'll reframe the cursor and try again. But restart the 112s so won't be last page -
        // have to restart the duplicate keys because the scrollId has been cleared with the pitId being closed
        page = Optional.ofNullable(page.cursor()).map(thisRepository::search).orElseThrow();
        assertFalse(page.lastPage());
        assertEquals(2, page.documents().size());
        assertEquals(112, page.documents().get(0).get("ts"));
        assertEquals(112, page.documents().get(1).get("ts"));

        // will be last page now though - only one entry left
        page = Optional.ofNullable(page.cursor()).map(thisRepository::search).orElseThrow();
        assertTrue(page.lastPage());
        assertEquals(1, page.documents().size());
    }

    @Test
    public void testHandlingReplaysDuplicatesRatherThanSkips() throws IOException, InterruptedException {
        deleteTestIndex();

        insertMockData(111);
        insertMockData(112);
        insertMockData(112);
        insertMockData(112);
        insertMockData(113);
        refreshIndex();

        final ElasticRepository thisRepository = new ElasticRepository(connection);
        thisRepository.setPageSize(2);
        final var initialCursor = Cursor.of(TEST_INDEX, List.of(new CursorField(CURSOR_FIELD, 0L)));

        var page = thisRepository.search(initialCursor);
        assertFalse(page.lastPage());

        // close the pit
        Optional.ofNullable(page.cursor()).ifPresent(cursor -> thisRepository.closePit(cursor.getPitId()));

        // should still work - it'll reframe the cursor and try again.
        thisRepository.setPageSize(4);
        page = Optional.ofNullable(page.cursor()).map(thisRepository::search).orElseThrow();
        assertTrue(page.lastPage());
        assertEquals(4, page.documents().size());
        assertEquals(112, page.documents().get(0).get("ts"));
        assertEquals(112, page.documents().get(1).get("ts"));
        assertEquals(112, page.documents().get(2).get("ts"));
        assertEquals(113, page.documents().get(3).get("ts"));
    }

    @Test(expected = ElasticsearchStatusException.class)
    public void testGracefulHandlingOfUnexpectedlyClosedPitIdLetsOtherIssuesPast()
        throws IOException, InterruptedException {
        deleteTestIndex();

        insertMockData(111);
        insertMockData(112);
        insertMockData(113);
        refreshIndex();

        final ElasticRepository thisRepository = new ElasticRepository(connection);
        thisRepository.setPageSize(2);
        final var initialCursor = Cursor.of(TEST_INDEX, List.of(new CursorField(CURSOR_FIELD, 0L)));

        var page = thisRepository.search(initialCursor);
        assertFalse(page.lastPage());

        // delete the test index all together
        deleteTestIndex();

        // should attempt a reframe, but fail and throw anyway
        Optional.ofNullable(page.cursor()).map(thisRepository::search).orElseThrow();
    }
}
