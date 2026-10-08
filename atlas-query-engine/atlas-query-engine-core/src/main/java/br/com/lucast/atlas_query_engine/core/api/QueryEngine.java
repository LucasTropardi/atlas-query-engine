package br.com.lucast.atlas_query_engine.core.api;

import br.com.lucast.atlas_query_engine.core.model.QueryRequest;
import br.com.lucast.atlas_query_engine.core.result.QueryResult;
import br.com.lucast.atlas_query_engine.core.result.QueryPreview;

public interface QueryEngine {

    QueryPreview preview(QueryRequest request);

    QueryResult execute(QueryRequest request);
}
