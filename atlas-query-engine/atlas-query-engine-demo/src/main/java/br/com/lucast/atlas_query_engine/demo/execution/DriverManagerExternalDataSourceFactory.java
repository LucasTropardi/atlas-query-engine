package br.com.lucast.atlas_query_engine.demo.execution;

import br.com.lucast.atlas_query_engine.demo.connection.entity.DatabaseType;
import br.com.lucast.atlas_query_engine.demo.connection.exception.JdbcConnectionConfigurationException;
import br.com.lucast.atlas_query_engine.demo.connection.exception.UnsupportedDatabaseTypeException;
import br.com.lucast.atlas_query_engine.demo.connection.model.ConnectionDefinition;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import javax.sql.DataSource;
import org.springframework.jdbc.datasource.DriverManagerDataSource;
import org.springframework.stereotype.Component;

@Component
public class DriverManagerExternalDataSourceFactory implements ExternalDataSourceFactory {

    private final Map<String, DataSource> cache = new ConcurrentHashMap<>();

    @Override
    public DataSource create(ConnectionDefinition connectionDefinition) {
        return cache.computeIfAbsent(connectionDefinition.connectionKey(), ignored -> {
            String jdbcUrl = buildJdbcUrl(connectionDefinition);
            DriverManagerDataSource dataSource = new DriverManagerDataSource();
            dataSource.setDriverClassName(resolveDriverClassName(connectionDefinition.dbType()));
            dataSource.setUrl(jdbcUrl);
            dataSource.setUsername(connectionDefinition.username());
            dataSource.setPassword(connectionDefinition.password());
            return dataSource;
        });
    }

    @Override
    public String buildJdbcUrl(ConnectionDefinition connectionDefinition) {
        validate(connectionDefinition);
        return switch (connectionDefinition.dbType()) {
            case POSTGRES -> "jdbc:postgresql://%s:%d/%s".formatted(
                    connectionDefinition.host(),
                    connectionDefinition.port(),
                    connectionDefinition.databaseName()
            );
            case MYSQL -> "jdbc:mysql://%s:%d/%s".formatted(
                    connectionDefinition.host(),
                    connectionDefinition.port(),
                    connectionDefinition.databaseName()
            );
            case ORACLE -> "jdbc:oracle:thin:@//%s:%d/%s".formatted(
                    connectionDefinition.host(),
                    connectionDefinition.port(),
                    connectionDefinition.databaseName()
            );
        };
    }

    private String resolveDriverClassName(DatabaseType dbType) {
        return switch (dbType) {
            case POSTGRES -> "org.postgresql.Driver";
            case MYSQL -> "com.mysql.cj.jdbc.Driver";
            case ORACLE -> "oracle.jdbc.OracleDriver";
        };
    }

    private void validate(ConnectionDefinition connectionDefinition) {
        if (connectionDefinition == null) {
            throw new JdbcConnectionConfigurationException("Connection definition must not be null");
        }
        if (connectionDefinition.dbType() == null) {
            throw new UnsupportedDatabaseTypeException("null");
        }
        if (isBlank(connectionDefinition.host())) {
            throw new JdbcConnectionConfigurationException("Host must not be blank");
        }
        if (connectionDefinition.port() <= 0) {
            throw new JdbcConnectionConfigurationException("Port must be greater than zero");
        }
        if (isBlank(connectionDefinition.databaseName())) {
            throw new JdbcConnectionConfigurationException("Database name must not be blank");
        }
        if (isBlank(connectionDefinition.username())) {
            throw new JdbcConnectionConfigurationException("Username must not be blank");
        }
    }

    private boolean isBlank(String value) {
        return value == null || value.isBlank();
    }
}
