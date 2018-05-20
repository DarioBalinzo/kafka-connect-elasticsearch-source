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


import com.github.dariobalinzo.schema.SchemaConverter;
import com.github.dariobalinzo.schema.StructConverter;
import com.github.dariobalinzo.task.ElasticSourceTask;
import com.github.dariobalinzo.utils.ElasticConnection;
import junit.framework.TestCase;
import org.apache.http.util.EntityUtils;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Response;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.builder.SearchSourceBuilder;

import java.io.IOException;
import java.util.Map;

public class TestQuery extends TestCase {

    private ElasticSourceTask task;

    public void setUp() throws Exception {

        task = new ElasticSourceTask();

    }



    public void testTask() throws Exception {

        task.setupTest("metricbeat-6.2.4-2018.05.20");
        //task.poll();

    }


    public void tearDown() throws Exception {

        task.stop();
    }

}
