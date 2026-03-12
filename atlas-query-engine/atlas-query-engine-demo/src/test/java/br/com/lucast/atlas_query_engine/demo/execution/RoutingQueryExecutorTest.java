package br.com.lucast.atlas_query_engine.demo.execution;

import br.com.lucast.atlas_query_engine.core.model.QueryRequest;
import br.com.lucast.atlas_query_engine.core.result.QueryResult;
import br.com.lucast.atlas_query_engine.core.translator.SqlQuery;
import br.com.lucast.atlas_query_engine.demo.connection.entity.DatabaseType;
import br.com.lucast.atlas_query_engine.demo.connection.exception.ExternalQueryConnectionException;
import java.io.PrintWriter;
import java.sql.Connection;
import java.sql.SQLException;
import java.sql.SQLFeatureNotSupportedException;
import java.util.List;
import java.util.logging.Logger;
import javax.sql.DataSource;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.CannotGetJdbcConnectionException;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class RoutingQueryExecutorTest {

    private EmbeddedDatabase defaultDatabase;
    private EmbeddedDatabase externalDatabase;

    @BeforeEach
    void setUp() {
        defaultDatabase = new EmbeddedDatabaseBuilder()
                .setType(EmbeddedDatabaseType.H2)
                .generateUniqueName(true)
                .addScript("classpath:test-sql/default-orders.sql")
                .build();

        externalDatabase = new EmbeddedDatabaseBuilder()
                .setType(EmbeddedDatabaseType.H2)
                .generateUniqueName(true)
                .addScript("classpath:test-sql/external-orders.sql")
                .build();
    }

    @AfterEach
    void tearDown() {
        if (defaultDatabase != null) {
            defaultDatabase.shutdown();
        }
        if (externalDatabase != null) {
            externalDatabase.shutdown();
        }
    }

    @Test
    void shouldUseDefaultDatasourceWhenConnectionIsNotProvided() {
        RoutingQueryExecutor executor = new RoutingQueryExecutor(new StubResolver(
                QueryExecutionContext.defaultDataSource(defaultDatabase)
        ));
        QueryRequest request = new QueryRequest();
        request.setDataset("orders");

        QueryResult result = executor.execute(request, new SqlQuery("SELECT country FROM orders", List.of()));

        assertThat(result.getRows()).containsExactly(List.of("BR"));
    }

    @Test
    void shouldUseExternalDatasourceWhenConnectionIsProvided() {
        RoutingQueryExecutor executor = new RoutingQueryExecutor(new StubResolver(
                QueryExecutionContext.externalDataSource(externalDatabase, "sales_mysql", DatabaseType.MYSQL)
        ));
        QueryRequest request = new QueryRequest();
        request.setDataset("orders");
        request.setConnection("sales_mysql");

        QueryResult result = executor.execute(request, new SqlQuery("SELECT country FROM orders", List.of()));

        assertThat(result.getRows()).containsExactly(List.of("US"));
    }

    @Test
    void shouldWrapExternalDatasourceFailures() {
        RoutingQueryExecutor executor = new RoutingQueryExecutor(new StubResolver(
                QueryExecutionContext.externalDataSource(new BrokenDataSource(), "oracle_finance", DatabaseType.ORACLE)
        ));
        QueryRequest request = new QueryRequest();
        request.setDataset("orders");
        request.setConnection("oracle_finance");

        assertThatThrownBy(() -> executor.execute(request, new SqlQuery("SELECT country FROM orders", List.of())))
                .isInstanceOf(ExternalQueryConnectionException.class)
                .hasMessage("Failed to connect or execute query using external connection: oracle_finance")
                .hasCauseInstanceOf(CannotGetJdbcConnectionException.class);
    }

    private static final class StubResolver extends QueryExecutionConnectionResolver {

        private final QueryExecutionContext context;

        private StubResolver(QueryExecutionContext context) {
            super(null, null, null);
            this.context = context;
        }

        @Override
        public QueryExecutionContext resolve(QueryRequest request) {
            return context;
        }
    }

    private static final class BrokenDataSource implements DataSource {

        @Override
        public Connection getConnection() throws SQLException {
            throw new SQLException("connection refused");
        }

        @Override
        public Connection getConnection(String username, String password) throws SQLException {
            throw new SQLException("connection refused");
        }

        @Override
        public <T> T unwrap(Class<T> iface) {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isWrapperFor(Class<?> iface) {
            return false;
        }

        @Override
        public PrintWriter getLogWriter() {
            return null;
        }

        @Override
        public void setLogWriter(PrintWriter out) {
        }

        @Override
        public void setLoginTimeout(int seconds) {
        }

        @Override
        public int getLoginTimeout() {
            return 0;
        }

        @Override
        public Logger getParentLogger() throws SQLFeatureNotSupportedException {
            throw new SQLFeatureNotSupportedException();
        }
    }
}
