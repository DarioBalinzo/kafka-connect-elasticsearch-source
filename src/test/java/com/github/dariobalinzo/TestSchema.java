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

package com.github.dariobalinzo;


import com.github.dariobalinzo.schema.SchemaConverter;
import com.github.dariobalinzo.schema.StructConverter;
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

public class TestSchema extends TestCase {

    private ElasticConnection es;

    public void setUp() throws Exception {
        es = new ElasticConnection("localhost", 9200, 10, 100);


    }

    public void testMapping() throws IOException {

        Response response = es.getClient().getLowLevelClient().performRequest("GET", "test/_mapping");
        String responseBody = EntityUtils.toString(response.getEntity());
        System.out.println(responseBody);


    }

    public void testSearch() throws Exception {
        SearchRequest searchRequest = new SearchRequest();
        SearchSourceBuilder searchSourceBuilder = new SearchSourceBuilder();
        searchSourceBuilder.query(QueryBuilders.matchAllQuery());
        searchRequest.source(searchSourceBuilder);
        searchRequest.indices("test");
        SearchResponse searchResponse = es.getClient().search(searchRequest);
        SearchHits hits = searchResponse.getHits();
        SearchHit[] searchHits = hits.getHits();
        for (SearchHit hit : searchHits) {
            // do something with the SearchHit
            Map<String, Object> sourceAsMap = hit.getSourceAsMap();
            System.out.println(sourceAsMap);
            Schema schema = SchemaConverter.convertElasticMapping2AvroSchema(sourceAsMap, "test");
            schema.toString();
            Struct struct = StructConverter.convertElasticDocument2AvroStruct(sourceAsMap,schema);
            struct.toString();
        }

    }


    public void tearDown() throws Exception {

        es.closeQuietly();
    }

}
