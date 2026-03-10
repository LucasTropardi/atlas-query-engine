package br.com.lucast.atlas_query_engine.core.api;

import br.com.lucast.atlas_query_engine.core.model.QueryRequest;
import br.com.lucast.atlas_query_engine.core.result.QueryResult;

public interface QueryEngine {

    QueryResult execute(QueryRequest request);
}
