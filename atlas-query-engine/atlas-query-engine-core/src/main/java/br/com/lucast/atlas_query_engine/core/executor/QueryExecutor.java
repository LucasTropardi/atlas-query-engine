package br.com.lucast.atlas_query_engine.core.executor;

import br.com.lucast.atlas_query_engine.core.model.QueryRequest;
import br.com.lucast.atlas_query_engine.core.result.QueryResult;
import br.com.lucast.atlas_query_engine.core.translator.SqlQuery;

public interface QueryExecutor {

    QueryResult execute(QueryRequest request, SqlQuery sqlQuery);
}
