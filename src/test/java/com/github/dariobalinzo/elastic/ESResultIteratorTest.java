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

import co.elastic.clients.elasticsearch._types.ElasticsearchException;
import co.elastic.clients.json.JsonData;
import com.github.dariobalinzo.TestContainersContext;
import com.github.dariobalinzo.elastic.response.Cursor;
import com.github.dariobalinzo.elastic.response.CursorField;
import org.elasticsearch.action.search.ClosePointInTimeRequest;
import org.elasticsearch.client.RequestOptions;
import org.junit.Test;

import java.io.IOException;
import java.util.List;

import static org.junit.Assert.*;


public class ESResultIteratorTest extends TestContainersContext {

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

        var thisRepository = new ElasticRepository(connection);
        thisRepository.setPageSize(2);
        final var initialCursor = Cursor.of(TEST_INDEX, List.of(new CursorField(CURSOR_FIELD, 0L)));

        var page = thisRepository.getIterator(initialCursor);

        assertEquals(111, page.next().getData().get("ts"));
        assertEquals(112, page.next().getData().get("ts"));
        assertEquals(113, page.next().getData().get("ts"));
        assertEquals(113, page.next().getData().get("ts"));
        assertEquals(113, page.next().getData().get("ts"));
        assertEquals(113, page.next().getData().get("ts"));
        assertEquals(114, page.next().getData().get("ts"));
    }

    @Test
    public void shouldCompleteWhenReachingEnd() throws IOException, InterruptedException {
        deleteTestIndex();

        insertMockData(111);
        insertMockData(112);
        refreshIndex();

        var thisRepository = new ElasticRepository(connection);
        thisRepository.setPageSize(2);
        final var initialCursor = Cursor.of(TEST_INDEX, List.of(new CursorField(CURSOR_FIELD, 0L)));

        var page = thisRepository.getIterator(initialCursor);

        assertEquals(111, page.next().getData().get("ts"));
        assertEquals(112, page.next().getData().get("ts"));
        assertFalse(page.hasNext());
    }

    private void closePit(ElasticConnection elasticConnection, String pitId) {
        if (pitId == null) {
            return;
        }

        ClosePointInTimeRequest closeRequest = new ClosePointInTimeRequest(pitId);
        try {
            elasticConnection.getClient().closePointInTime(closeRequest, RequestOptions.DEFAULT);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Test
    public void testThrowsOnUnexpectedlyClosedPIT() throws IOException, InterruptedException {
        deleteTestIndex();

        insertMockData(111);
        insertMockData(112);
        insertMockData(112);
        insertMockData(112);
        refreshIndex();

        final ElasticRepository thisRepository = new ElasticRepository(connection);
        thisRepository.setPageSize(2);
        final var initialCursor = Cursor.of(TEST_INDEX, List.of(new CursorField(CURSOR_FIELD, 0L)));

        var page = thisRepository.getIterator(initialCursor);

        // iterate to the end of the page
        page.next();
        page.next();

        // close the pit
        closePit(thisRepository.getElasticConnection(), page.getCursor().getPitId());

        // should throw
        try {
            page.next();
            fail("Should have thrown");
        } catch (RuntimeException e) {
            assertTrue(e.getCause() instanceof ElasticsearchException);
        }
    }
}
