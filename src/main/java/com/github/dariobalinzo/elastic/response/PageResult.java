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

package com.github.dariobalinzo.elastic.response;


import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;

import java.util.*;

public final class PageResult implements Iterator<PageResult.Document> {

    private final SearchHits searchHits;
    private final boolean isLastPage;

    private int currentIndex = 0;
    private Cursor cursor;

    public PageResult(SearchHits searchHits, Cursor openingCursor) {
        this.searchHits = searchHits;
        this.cursor = openingCursor;

        this.isLastPage = this.searchHits.getHits().length +
                this.cursor.getRunningDocumentCount() >= this.searchHits.getTotalHits().value;
    }

    @Override
    public boolean hasNext() {
        return currentIndex < searchHits.getHits().length;
    }

    @Override
    public Document next() {
        if (hasNext()) {
            cursor = cursor.scroll(searchHits.getAt(currentIndex).getSortValues(), 1);
            var result = new Document(searchHits.getAt(currentIndex), cursor);
            currentIndex++;

            return result;
        }

        return null;
    }

    public boolean isLastPage() {
        return isLastPage;
    }


    public Cursor cursor() {
        return cursor;
    }

    public int getSize() {
        if (searchHits == null) {
            return 0;
        }
        return searchHits.getHits().length;
    }

    public String getIndex() {
        return cursor.getIndex();
    }

    public static class Document {
        private final Map<String, Object> documentMap;
        private final Cursor cursor;
        private final String id;

        public Document(SearchHit document, Cursor cursor) {
            this.documentMap = document.getSourceAsMap();
            this.documentMap.put("es-id", document.getId());
            this.documentMap.put("es-index", document.getIndex());
            this.cursor = cursor;
            this.id = document.getId();
        }

        public String getId() {
            return this.id;
        }

        public Map<String, Object> asMap() {
            return documentMap;
        }

        public Cursor getDocumentCursor() {
            return cursor;
        }
    }
}
