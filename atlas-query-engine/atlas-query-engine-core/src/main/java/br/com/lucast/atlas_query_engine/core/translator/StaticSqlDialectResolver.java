package br.com.lucast.atlas_query_engine.core.translator;

import br.com.lucast.atlas_query_engine.core.model.QueryRequest;

public class StaticSqlDialectResolver implements SqlDialectResolver {

    private final SqlDialect sqlDialect;

    public StaticSqlDialectResolver(SqlDialect sqlDialect) {
        this.sqlDialect = sqlDialect;
    }

    @Override
    public SqlDialect resolve(QueryRequest request) {
        return sqlDialect;
    }
}
