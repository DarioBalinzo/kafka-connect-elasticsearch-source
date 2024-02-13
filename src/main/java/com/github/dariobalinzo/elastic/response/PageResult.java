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


import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public final class PageResult {

    private final List<Map<String, Object>> documents;
    private final Cursor cursor;
    private final boolean lastPage;

    public PageResult(List<Map<String, Object>> documents, Cursor cursor, boolean lastPage) {
        this.documents = documents;
        this.cursor = cursor;
        this.lastPage = lastPage;
    }

    public static PageResult intermediatePage(List<Map<String, Object>> documents, Cursor cursor) {
        return new PageResult(documents, cursor, false);
    }

    public static PageResult lastPage(List<Map<String, Object>> documents, Cursor cursor) {
        return new PageResult(documents, cursor, true);
    }

    public static PageResult empty(Cursor cursor) {
        return new PageResult(new ArrayList<>(), cursor, true);
    }

    public boolean isEmpty() {
        return documents == null || documents.isEmpty();
    }

    public List<Map<String, Object>> documents() {
        return documents;
    }

    public Cursor cursor() {
        return cursor;
    }

    public boolean lastPage() {
        return lastPage;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == this) {
            return true;
        }
        if (obj == null || obj.getClass() != this.getClass()) {
            return false;
        }
        var that = (PageResult) obj;
        return Objects.equals(this.documents, that.documents) && Objects.equals(this.cursor, that.cursor)
            && this.lastPage == that.lastPage;
    }

    @Override
    public int hashCode() {
        return Objects.hash(documents, cursor, lastPage);
    }

    @Override
    public String toString() {
        return "PageResult[" + "documents=" + documents + ", " + "cursor=" + cursor + ", " + "lastPage=" + lastPage
            + ']';
    }

}
