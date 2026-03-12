package br.com.lucast.atlas_query_engine.demo.execution;

import br.com.lucast.atlas_query_engine.core.model.QueryRequest;
import br.com.lucast.atlas_query_engine.core.translator.MySqlSqlDialect;
import br.com.lucast.atlas_query_engine.core.translator.OracleSqlDialect;
import br.com.lucast.atlas_query_engine.core.translator.PostgresSqlDialect;
import br.com.lucast.atlas_query_engine.core.translator.SqlDialect;
import br.com.lucast.atlas_query_engine.core.translator.SqlDialectResolver;
import br.com.lucast.atlas_query_engine.demo.config.AtlasSqlProperties;
import br.com.lucast.atlas_query_engine.demo.connection.entity.DatabaseType;
import br.com.lucast.atlas_query_engine.demo.connection.exception.UnsupportedDatabaseTypeException;
import br.com.lucast.atlas_query_engine.demo.connection.model.ConnectionDefinition;
import br.com.lucast.atlas_query_engine.demo.connection.service.ConnectionRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class DefaultSqlDialectResolver implements SqlDialectResolver {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultSqlDialectResolver.class);

    private final AtlasSqlProperties atlasSqlProperties;
    private final ConnectionRegistry connectionRegistry;

    public DefaultSqlDialectResolver(AtlasSqlProperties atlasSqlProperties, ConnectionRegistry connectionRegistry) {
        this.atlasSqlProperties = atlasSqlProperties;
        this.connectionRegistry = connectionRegistry;
    }

    @Override
    public SqlDialect resolve(QueryRequest request) {
        if (request.getConnection() == null || request.getConnection().isBlank()) {
            DatabaseType defaultDialect = atlasSqlProperties.getDefaultDialect();
            LOGGER.info("Resolved default SQL dialect={} for target={}", defaultDialect, request.getTargetName());
            return mapDialect(defaultDialect);
        }

        ConnectionDefinition connectionDefinition = connectionRegistry.resolveByConnectionKey(request.getConnection());
        LOGGER.info(
                "Resolved SQL dialect from connection key={} as dbType={}",
                connectionDefinition.connectionKey(),
                connectionDefinition.dbType()
        );
        return mapDialect(connectionDefinition.dbType());
    }

    private SqlDialect mapDialect(DatabaseType dbType) {
        return switch (dbType) {
            case POSTGRES -> new PostgresSqlDialect();
            case MYSQL -> new MySqlSqlDialect();
            case ORACLE -> new OracleSqlDialect();
            default -> throw new UnsupportedDatabaseTypeException(String.valueOf(dbType));
        };
    }
}
