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

        assertEquals(2, page.getSize());
        assertEquals(111, page.next().asMap().get("ts"));
        assertEquals(112, page.next().asMap().get("ts"));

        page = Optional.ofNullable(page.cursor()).map(thisRepository::search).orElseThrow();

        assertEquals(2, page.getSize());
        assertEquals(113, page.next().asMap().get("ts"));
        assertEquals(113, page.next().asMap().get("ts"));

        page = Optional.ofNullable(page.cursor()).map(thisRepository::search).orElseThrow();

        assertEquals(2, page.getSize());
        assertEquals(113, page.next().asMap().get("ts"));
        assertEquals(113, page.next().asMap().get("ts"));

        page = Optional.ofNullable(page.cursor()).map(thisRepository::search).orElseThrow();
        assertTrue(page.isLastPage());
        assertEquals(1, page.getSize());
        assertEquals(114, page.next().asMap().get("ts"));

        //        page = Optional.ofNullable(page.cursor()).map(thisRepository::search).orElseThrow();
        //        assertTrue(page.isEmpty());

        deleteTestIndex();
        insertMockData(115);
        insertMockData(116);
        insertMockData(116);
        refreshIndex();

        page = Optional.ofNullable(page.cursor()).map(thisRepository::search).orElseThrow();
        assertEquals(2, page.getSize());
    }

    @Test
    public void shouldCompleteWhenReachingEnd() throws IOException, InterruptedException {
        deleteTestIndex();

        insertMockData(111);
        insertMockData(112);
        refreshIndex();

        ElasticRepository thisRepository = new ElasticRepository(connection);
        thisRepository.setPageSize(2);
        final var initialCursor = Cursor.of(TEST_INDEX, List.of(new CursorField(CURSOR_FIELD, 0L)));

        var page = thisRepository.search(initialCursor);

        assertEquals(2, page.getSize());
        assertEquals(111, page.next().asMap().get("ts"));
        assertEquals(112, page.next().asMap().get("ts"));

        page = Optional.ofNullable(page.cursor()).map(thisRepository::search).orElseThrow();
        assertFalse(page.hasNext());
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
        assertFalse(page.isLastPage());
        page.next();

        // close the pit
        Optional.ofNullable(page.cursor()).ifPresent(cursor -> thisRepository.closePit(cursor.getPitId()));

        // should still work - it'll reframe the cursor and try again. But restart the 112s so won't be last page -
        // have to restart the duplicate keys because the scrollId has been cleared with the pitId being closed
        page = Optional.ofNullable(page.cursor()).map(thisRepository::search).orElseThrow();
        assertFalse(page.isLastPage());
        assertEquals(2, page.getSize());
        assertEquals(111, page.next().asMap().get("ts"));
        assertEquals(112, page.next().asMap().get("ts"));

        // will be last page now though - only one entry left
        page = Optional.ofNullable(page.cursor()).map(thisRepository::search).orElseThrow();
        assertTrue(page.isLastPage());
        assertEquals(2, page.getSize());
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
        assertFalse(page.isLastPage());
        page.next();
        page.next();

        // close the pit
        Optional.ofNullable(page.cursor()).ifPresent(cursor -> thisRepository.closePit(cursor.getPitId()));

        // should still work - it'll reframe the cursor and try again.
        thisRepository.setPageSize(4);
        page = Optional.ofNullable(page.cursor()).map(thisRepository::search).orElseThrow();
        assertTrue(page.isLastPage());
        assertEquals(4, page.getSize());
        assertEquals(112, page.next().asMap().get("ts"));
        assertEquals(112, page.next().asMap().get("ts"));
        assertEquals(112, page.next().asMap().get("ts"));
        assertEquals(113, page.next().asMap().get("ts"));
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
        assertFalse(page.isLastPage());

        // delete the test index all together
        deleteTestIndex();

        // should attempt a reframe, but fail and throw anyway
        Optional.ofNullable(page.cursor()).map(thisRepository::search).orElseThrow();
    }
}
