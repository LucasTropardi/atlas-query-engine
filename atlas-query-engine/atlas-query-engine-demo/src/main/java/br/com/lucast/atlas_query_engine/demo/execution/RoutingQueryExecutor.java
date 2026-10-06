package br.com.lucast.atlas_query_engine.demo.execution;

import br.com.lucast.atlas_query_engine.core.executor.JdbcQueryExecutor;
import br.com.lucast.atlas_query_engine.core.executor.QueryExecutor;
import br.com.lucast.atlas_query_engine.core.executor.QueryExecutionResolver;
import br.com.lucast.atlas_query_engine.core.executor.ResolvedQueryExecution;
import br.com.lucast.atlas_query_engine.core.model.QueryRequest;
import br.com.lucast.atlas_query_engine.core.result.QueryResult;
import br.com.lucast.atlas_query_engine.core.translator.SqlQuery;
import br.com.lucast.atlas_query_engine.demo.connection.exception.ExternalQueryConnectionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessResourceFailureException;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.stereotype.Component;

@Component
public class RoutingQueryExecutor implements QueryExecutor, QueryExecutionResolver {

    private static final Logger LOGGER = LoggerFactory.getLogger(RoutingQueryExecutor.class);

    private final QueryExecutionConnectionResolver connectionResolver;

    private final DefaultSqlDialectResolver dialectResolver;

    public RoutingQueryExecutor(QueryExecutionConnectionResolver connectionResolver, DefaultSqlDialectResolver dialectResolver) {
        this.connectionResolver = connectionResolver;
        this.dialectResolver = dialectResolver;
    }

    @Override
    public ResolvedQueryExecution resolve(QueryRequest request) {
        QueryExecutionContext context = connectionResolver.resolve(request);
        return new ResolvedQueryExecution(dialectResolver.resolve(context.dbType()),
                (query, sql) -> execute(context, query, sql));
    }

    @Override
    public QueryResult execute(QueryRequest request, SqlQuery sqlQuery) {
        return resolve(request).executor().execute(request, sqlQuery);
    }

    private QueryResult execute(QueryExecutionContext context, QueryRequest request, SqlQuery sqlQuery) {
        try {
            if (context.external()) {
                LOGGER.info(
                        "Executing query for target={} using external connection key={} dbType={}",
                        request.getTargetName(),
                        context.connectionKey(),
                        context.dbType()
                );
            } else {
                LOGGER.info("Executing query for target={} using default datasource", request.getTargetName());
            }

            JdbcQueryExecutor delegate = new JdbcQueryExecutor(new JdbcTemplate(context.dataSource()));
            return delegate.execute(request, sqlQuery);
        } catch (DataAccessResourceFailureException exception) {
            if (context.external()) {
                LOGGER.error(
                        "Failed to execute query for target={} using external connection key={}",
                        request.getTargetName(),
                        context.connectionKey(),
                        exception
                );
                throw new ExternalQueryConnectionException(context.connectionKey(), exception);
            }
            throw exception;
        }
    }
}
