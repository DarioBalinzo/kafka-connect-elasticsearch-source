package com.github.dariobalinzo.elastic;

import org.apache.http.HttpHost;
import org.elasticsearch.client.Request;
import org.elasticsearch.client.Response;
import org.elasticsearch.client.RestClient;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.testcontainers.elasticsearch.ElasticsearchContainer;

import java.io.IOException;

public class ElasticRepositoryTest {

    private static final String ELASTICSEARCH_IMAGE = "docker.elastic.co/elasticsearch/elasticsearch:7.9.3";
    private static ElasticsearchContainer container;

    @BeforeClass
    public static void setupElastic() {
        // Create the elasticsearch container.
      container = new ElasticsearchContainer(ELASTICSEARCH_IMAGE);
      container.start();
    }


    @Test
    public void shouldFetchDataFromElastic() throws IOException {
        // Create the secured client.
        RestClient client = RestClient
                .builder(HttpHost.create(container.getHttpHostAddress()))
                .build();

        Response response = client.performRequest(new Request("GET", "/_cluster/health"));
    }

    @AfterClass
    public static void stopElastic() {
        if (container != null) {
            container.close();
        }
    }

}
