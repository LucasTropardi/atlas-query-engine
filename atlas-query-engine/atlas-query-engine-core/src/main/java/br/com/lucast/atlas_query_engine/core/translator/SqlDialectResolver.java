package br.com.lucast.atlas_query_engine.core.translator;

import br.com.lucast.atlas_query_engine.core.model.QueryRequest;

public interface SqlDialectResolver {

    SqlDialect resolve(QueryRequest request);
}
