package br.com.lucast.atlas_query_engine.demo.execution;

import br.com.lucast.atlas_query_engine.core.model.QueryRequest;
import br.com.lucast.atlas_query_engine.demo.connection.entity.DatabaseType;
import br.com.lucast.atlas_query_engine.demo.connection.exception.ConnectionNotFoundException;
import br.com.lucast.atlas_query_engine.demo.connection.exception.InactiveConnectionException;
import br.com.lucast.atlas_query_engine.demo.connection.model.ConnectionDefinition;
import br.com.lucast.atlas_query_engine.demo.connection.service.ConnectionRegistry;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class QueryExecutionConnectionResolverTest {

    @Test
    void shouldUseDefaultDatasourceWhenConnectionIsMissing() {
        DataSource defaultDataSource = new DriverManagerDataSource();
        QueryExecutionConnectionResolver resolver = new QueryExecutionConnectionResolver(
                defaultDataSource,
                new StubConnectionRegistry(null, null),
                new StubExternalDataSourceFactory(null)
        );

        QueryRequest request = new QueryRequest();
        request.setDataset("orders");

        QueryExecutionContext context = resolver.resolve(request);

        assertThat(context.external()).isFalse();
        assertThat(context.dataSource()).isSameAs(defaultDataSource);
    }

    @Test
    void shouldResolveExternalDatasourceWhenConnectionIsProvided() {
        DataSource defaultDataSource = new DriverManagerDataSource();
        DataSource externalDataSource = new DriverManagerDataSource();
        ConnectionDefinition definition = new ConnectionDefinition(
                "sales_mysql",
                DatabaseType.MYSQL,
                "sales-db.internal",
                3306,
                "sales",
                "sales_user",
                "sales_pass"
        );
        QueryExecutionConnectionResolver resolver = new QueryExecutionConnectionResolver(
                defaultDataSource,
                new StubConnectionRegistry(definition, null),
                new StubExternalDataSourceFactory(externalDataSource)
        );

        QueryRequest request = new QueryRequest();
        request.setConnection("sales_mysql");
        request.setDataset("orders");

        QueryExecutionContext context = resolver.resolve(request);

        assertThat(context.external()).isTrue();
        assertThat(context.connectionKey()).isEqualTo("sales_mysql");
        assertThat(context.dbType()).isEqualTo(DatabaseType.MYSQL);
        assertThat(context.dataSource()).isSameAs(externalDataSource);
    }

    @Test
    void shouldFailWhenConnectionIsNotFound() {
        QueryExecutionConnectionResolver resolver = new QueryExecutionConnectionResolver(
                new DriverManagerDataSource(),
                new StubConnectionRegistry(null, new ConnectionNotFoundException("missing")),
                new StubExternalDataSourceFactory(null)
        );

        QueryRequest request = new QueryRequest();
        request.setConnection("missing");
        request.setDataset("orders");

        assertThatThrownBy(() -> resolver.resolve(request))
                .isInstanceOf(ConnectionNotFoundException.class)
                .hasMessage("Connection not found for key: missing");
    }

    @Test
    void shouldFailWhenConnectionIsInactive() {
        QueryExecutionConnectionResolver resolver = new QueryExecutionConnectionResolver(
                new DriverManagerDataSource(),
                new StubConnectionRegistry(null, new InactiveConnectionException("legacy_oracle")),
                new StubExternalDataSourceFactory(null)
        );

        QueryRequest request = new QueryRequest();
        request.setConnection("legacy_oracle");
        request.setDataset("orders");

        assertThatThrownBy(() -> resolver.resolve(request))
                .isInstanceOf(InactiveConnectionException.class)
                .hasMessage("Connection is inactive for key: legacy_oracle");
    }

    private static final class StubConnectionRegistry extends ConnectionRegistry {

        private final ConnectionDefinition definition;
        private final RuntimeException exception;

        private StubConnectionRegistry(ConnectionDefinition definition, RuntimeException exception) {
            super(null, null);
            this.definition = definition;
            this.exception = exception;
        }

        @Override
        public ConnectionDefinition resolveByConnectionKey(String connectionKey) {
            if (exception != null) {
                throw exception;
            }
            return definition;
        }
    }

    private static final class StubExternalDataSourceFactory implements ExternalDataSourceFactory {

        private final DataSource dataSource;

        private StubExternalDataSourceFactory(DataSource dataSource) {
            this.dataSource = dataSource;
        }

        @Override
        public DataSource create(ConnectionDefinition connectionDefinition) {
            return dataSource;
        }

        @Override
        public String buildJdbcUrl(ConnectionDefinition connectionDefinition) {
            return "jdbc:test";
        }
    }
}
