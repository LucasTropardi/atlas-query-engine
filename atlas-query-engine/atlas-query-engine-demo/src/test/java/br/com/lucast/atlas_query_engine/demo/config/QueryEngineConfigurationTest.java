package br.com.lucast.atlas_query_engine.demo.config;

import br.com.lucast.atlas_query_engine.core.api.QueryEngine;
import br.com.lucast.atlas_query_engine.core.model.QueryRequest;
import br.com.lucast.atlas_query_engine.demo.connection.entity.DatabaseType;
import br.com.lucast.atlas_query_engine.demo.connection.model.ConnectionDefinition;
import br.com.lucast.atlas_query_engine.demo.connection.service.ConnectionRegistry;
import br.com.lucast.atlas_query_engine.demo.execution.*;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.core.env.MapPropertySource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.embedded.*;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;
import static org.assertj.core.api.Assertions.assertThat;

class QueryEngineConfigurationTest {
    @Test
    void shouldWireEngineAndResolveExternalConnectionOncePerQuery() {
        var database = new EmbeddedDatabaseBuilder().setType(EmbeddedDatabaseType.H2).generateUniqueName(true).build();
        var calls = new AtomicInteger();
        try (var context = new AnnotationConfigApplicationContext()) {
            new JdbcTemplate(database).execute("CREATE TABLE \"orders\" (\"id\" INTEGER)");
            new JdbcTemplate(database).execute("INSERT INTO \"orders\" VALUES (7)");
            context.getEnvironment().getPropertySources().addFirst(new MapPropertySource("test",
                    Map.of("atlas.security.encryption-key", "test-key")));
            context.registerBean(DataSource.class, () -> database);
            context.registerBean(LocalValidatorFactoryBean.class, LocalValidatorFactoryBean::new);
            context.registerBean(ConnectionRegistry.class, () -> new ConnectionRegistry(null, null) {
                @Override
                public ConnectionDefinition resolveByConnectionKey(String key) {
                    calls.incrementAndGet();
                    return new ConnectionDefinition(key, DatabaseType.POSTGRES, "host", 5432, "db", "user", "pass");
                }
            });
            context.registerBean(ExternalDataSourceFactory.class, () -> new ExternalDataSourceFactory() {
                @Override
                public DataSource create(ConnectionDefinition definition) { return database; }
                @Override
                public String buildJdbcUrl(ConnectionDefinition definition) { return "unused"; }
            });
            context.register(QueryEngineConfiguration.class, DefaultSqlDialectResolver.class,
                    QueryExecutionConnectionResolver.class, RoutingQueryExecutor.class);
            context.refresh();
            QueryRequest request = new QueryRequest();
            request.setTable("orders");
            request.setSelect(List.of("id"));
            request.setConnection("external");
            assertThat(context.getBean(QueryEngine.class).execute(request).getRows()).containsExactly(List.of(7));
            assertThat(calls).hasValue(1);
        } finally {
            database.shutdown();
        }
    }
}
