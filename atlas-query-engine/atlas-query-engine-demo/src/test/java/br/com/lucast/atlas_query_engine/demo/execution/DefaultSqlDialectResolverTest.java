package br.com.lucast.atlas_query_engine.demo.execution;

import br.com.lucast.atlas_query_engine.core.model.QueryRequest;
import br.com.lucast.atlas_query_engine.core.translator.MySqlSqlDialect;
import br.com.lucast.atlas_query_engine.core.translator.OracleSqlDialect;
import br.com.lucast.atlas_query_engine.core.translator.PostgresSqlDialect;
import br.com.lucast.atlas_query_engine.demo.config.AtlasSqlProperties;
import br.com.lucast.atlas_query_engine.demo.connection.entity.DatabaseType;
import br.com.lucast.atlas_query_engine.demo.connection.model.ConnectionDefinition;
import br.com.lucast.atlas_query_engine.demo.connection.service.ConnectionRegistry;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DefaultSqlDialectResolverTest {

    @Test
    void shouldResolveDefaultDialectWhenConnectionIsMissing() {
        AtlasSqlProperties properties = new AtlasSqlProperties();
        properties.setDefaultDialect(DatabaseType.POSTGRES);
        DefaultSqlDialectResolver resolver = new DefaultSqlDialectResolver(
                properties,
                new StubConnectionRegistry(null)
        );

        QueryRequest request = new QueryRequest();
        request.setDataset("orders");

        assertThat(resolver.resolve(request)).isInstanceOf(PostgresSqlDialect.class);
    }

    @Test
    void shouldResolveDialectFromExternalPostgresConnection() {
        assertThat(resolverFor(DatabaseType.POSTGRES).resolve(requestWithConnection()))
                .isInstanceOf(PostgresSqlDialect.class);
    }

    @Test
    void shouldResolveDialectFromExternalMysqlConnection() {
        assertThat(resolverFor(DatabaseType.MYSQL).resolve(requestWithConnection()))
                .isInstanceOf(MySqlSqlDialect.class);
    }

    @Test
    void shouldResolveDialectFromExternalOracleConnection() {
        assertThat(resolverFor(DatabaseType.ORACLE).resolve(requestWithConnection()))
                .isInstanceOf(OracleSqlDialect.class);
    }

    private DefaultSqlDialectResolver resolverFor(DatabaseType dbType) {
        AtlasSqlProperties properties = new AtlasSqlProperties();
        properties.setDefaultDialect(DatabaseType.POSTGRES);
        return new DefaultSqlDialectResolver(
                properties,
                new StubConnectionRegistry(new ConnectionDefinition(
                        "external",
                        dbType,
                        "host",
                        1234,
                        "db",
                        "user",
                        "pass"
                ))
        );
    }

    private QueryRequest requestWithConnection() {
        QueryRequest request = new QueryRequest();
        request.setConnection("external");
        request.setDataset("orders");
        return request;
    }

    private static final class StubConnectionRegistry extends ConnectionRegistry {

        private final ConnectionDefinition definition;

        private StubConnectionRegistry(ConnectionDefinition definition) {
            super(null, null);
            this.definition = definition;
        }

        @Override
        public ConnectionDefinition resolveByConnectionKey(String connectionKey) {
            return definition;
        }
    }
}
