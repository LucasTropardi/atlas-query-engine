package br.com.lucast.atlas_query_engine.demo.execution;

import br.com.lucast.atlas_query_engine.core.executor.JdbcQueryExecutor;
import br.com.lucast.atlas_query_engine.core.executor.QueryExecutor;
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
public class RoutingQueryExecutor implements QueryExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(RoutingQueryExecutor.class);

    private final QueryExecutionConnectionResolver connectionResolver;

    public RoutingQueryExecutor(QueryExecutionConnectionResolver connectionResolver) {
        this.connectionResolver = connectionResolver;
    }

    @Override
    public QueryResult execute(QueryRequest request, SqlQuery sqlQuery) {
        QueryExecutionContext context = connectionResolver.resolve(request);

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
