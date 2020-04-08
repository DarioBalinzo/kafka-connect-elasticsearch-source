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

package com.github.dariobalinzo;

import com.github.dariobalinzo.task.ElasticSourceTask;
import junit.framework.TestCase;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.Arrays;
import java.util.List;

public class TestQuery extends TestCase {

    private static final String testEsHost = "westmalle.acht.athena.zone";
    private static final int testEsPort = 30010;
    private static final String query = "category: \"diagnostics\"";

    private ElasticSourceTask task;

    public void setUp() throws Exception {
        task = new ElasticSourceTask();
    }

    public void testTask() throws Exception {
        task.setupTest(
                Arrays.asList("logstash"),
                testEsHost, testEsPort, query);

        List<SourceRecord> results = task.poll();
        results.forEach(System.out::println);
        System.out.println("\nHits: " + results.size());
    }

    public void tearDown() throws Exception {
        task.stop();
    }
}
