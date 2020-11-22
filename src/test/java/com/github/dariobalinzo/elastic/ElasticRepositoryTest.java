package com.github.dariobalinzo.elastic;

import com.github.dariobalinzo.TestContainersContext;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;

public class ElasticRepositoryTest extends TestContainersContext {


    @Test
    public void shouldFetchDataFromElastic() throws IOException, InterruptedException {
        deleteTestIndex();

        insertMockData(111);
        insertMockData(112);
        insertMockData(113);
        insertMockData(114);
        refreshIndex();

        PageResult firstPage = repository.searchAfter(TEST_INDEX, null);
        assertEquals(3, firstPage.getDocuments().size());

        PageResult secondPage = repository.searchAfter(TEST_INDEX, firstPage.getLastCursor());
        assertEquals(1, secondPage.getDocuments().size());

        PageResult emptyPage = repository.searchAfter(TEST_INDEX, secondPage.getLastCursor());
        assertEquals(0, emptyPage.getDocuments().size());
        assertNull(emptyPage.getLastCursor());

        assertEquals(Collections.singletonList(TEST_INDEX), repository.catIndices("source"));
        assertEquals(Collections.emptyList(), repository.catIndices("non-existing"));
    }

}
