package br.com.lucast.atlas_query_engine.demo.execution;

import br.com.lucast.atlas_query_engine.core.model.QueryRequest;
import br.com.lucast.atlas_query_engine.demo.connection.model.ConnectionDefinition;
import br.com.lucast.atlas_query_engine.demo.connection.service.ConnectionRegistry;
import javax.sql.DataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class QueryExecutionConnectionResolver {

    private static final Logger LOGGER = LoggerFactory.getLogger(QueryExecutionConnectionResolver.class);

    private final DataSource defaultDataSource;
    private final ConnectionRegistry connectionRegistry;
    private final ExternalDataSourceFactory externalDataSourceFactory;

    public QueryExecutionConnectionResolver(
            DataSource defaultDataSource,
            ConnectionRegistry connectionRegistry,
            ExternalDataSourceFactory externalDataSourceFactory
    ) {
        this.defaultDataSource = defaultDataSource;
        this.connectionRegistry = connectionRegistry;
        this.externalDataSourceFactory = externalDataSourceFactory;
    }

    public QueryExecutionContext resolve(QueryRequest request) {
        if (request.getConnection() == null || request.getConnection().isBlank()) {
            LOGGER.info("Using default datasource for target={}", request.getTargetName());
            return QueryExecutionContext.defaultDataSource(defaultDataSource);
        }

        try {
            LOGGER.info("Resolving external connection key={} for target={}", request.getConnection(), request.getTargetName());
            ConnectionDefinition connectionDefinition = connectionRegistry.resolveByConnectionKey(request.getConnection());
            LOGGER.info(
                    "Resolved external connection key={} with dbType={}",
                    connectionDefinition.connectionKey(),
                    connectionDefinition.dbType()
            );

            DataSource dataSource = externalDataSourceFactory.create(connectionDefinition);
            return QueryExecutionContext.externalDataSource(
                    dataSource,
                    connectionDefinition.connectionKey(),
                    connectionDefinition.dbType()
            );
        } catch (RuntimeException exception) {
            LOGGER.warn(
                    "Failed to resolve execution connection key={} for target={}: {}",
                    request.getConnection(),
                    request.getTargetName(),
                    exception.getMessage()
            );
            throw exception;
        }
    }
}
