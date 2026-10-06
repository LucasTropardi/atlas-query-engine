package br.com.lucast.atlas_query_engine.demo.execution;

import br.com.lucast.atlas_query_engine.core.translator.MySqlSqlDialect;
import br.com.lucast.atlas_query_engine.core.translator.OracleSqlDialect;
import br.com.lucast.atlas_query_engine.core.translator.PostgresSqlDialect;
import br.com.lucast.atlas_query_engine.core.translator.SqlDialect;
import br.com.lucast.atlas_query_engine.demo.config.AtlasSqlProperties;
import br.com.lucast.atlas_query_engine.demo.connection.entity.DatabaseType;
import org.springframework.stereotype.Component;

@Component
public class DefaultSqlDialectResolver {
    private final AtlasSqlProperties properties;

    public DefaultSqlDialectResolver(AtlasSqlProperties properties) {
        this.properties = properties;
    }

    public SqlDialect resolve(DatabaseType databaseType) {
        DatabaseType effectiveType = databaseType == null ? properties.getDefaultDialect() : databaseType;
        return switch (effectiveType) {
            case POSTGRES -> new PostgresSqlDialect();
            case MYSQL -> new MySqlSqlDialect();
            case ORACLE -> new OracleSqlDialect();
        };
    }
}
