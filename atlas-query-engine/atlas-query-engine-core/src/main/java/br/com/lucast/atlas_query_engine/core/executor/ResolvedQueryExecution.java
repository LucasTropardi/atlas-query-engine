package br.com.lucast.atlas_query_engine.core.executor;

import br.com.lucast.atlas_query_engine.core.translator.SqlDialect;
import java.util.Objects;

/** Dialect and executor resolved from the same connection configuration. */
public record ResolvedQueryExecution(SqlDialect dialect, QueryExecutor executor) {
    public ResolvedQueryExecution {
        Objects.requireNonNull(dialect, "dialect");
        Objects.requireNonNull(executor, "executor");
    }
}
