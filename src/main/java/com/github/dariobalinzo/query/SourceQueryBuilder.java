package com.github.dariobalinzo.query;

import com.github.dariobalinzo.ElasticSourceConnectorConfig;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

public class SourceQueryBuilder {
    private String query;

    public SourceQueryBuilder(String query) {
        this.query = query;
    }

    public SourceQueryBuilder(ElasticSourceConnectorConfig config) {
        query = config.getString(ElasticSourceConnectorConfig.ES_QUERY);
    }

    public QueryBuilder getSelectQuery() {
        return QueryBuilders.boolQuery().must(QueryBuilders.queryStringQuery(query));
    }
}
