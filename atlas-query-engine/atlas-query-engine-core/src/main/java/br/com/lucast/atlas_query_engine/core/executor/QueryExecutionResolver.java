package br.com.lucast.atlas_query_engine.core.executor;

import br.com.lucast.atlas_query_engine.core.model.QueryRequest;

@FunctionalInterface
public interface QueryExecutionResolver {
    ResolvedQueryExecution resolve(QueryRequest request);
}
