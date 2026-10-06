package br.com.lucast.atlas_query_engine.demo.execution;

import br.com.lucast.atlas_query_engine.core.translator.*;
import br.com.lucast.atlas_query_engine.demo.config.AtlasSqlProperties;
import br.com.lucast.atlas_query_engine.demo.connection.entity.DatabaseType;
import org.junit.jupiter.api.Test;
import static org.assertj.core.api.Assertions.assertThat;

class DefaultSqlDialectResolverTest {
    @Test
    void shouldUseConfiguredDefaultDialect() {
        var properties = new AtlasSqlProperties();
        properties.setDefaultDialect(DatabaseType.MYSQL);
        assertThat(new DefaultSqlDialectResolver(properties).resolve(null)).isInstanceOf(MySqlSqlDialect.class);
    }

    @Test
    void shouldUseDatabaseTypeFromResolvedConnection() {
        var resolver = new DefaultSqlDialectResolver(new AtlasSqlProperties());
        assertThat(resolver.resolve(DatabaseType.POSTGRES)).isInstanceOf(PostgresSqlDialect.class);
        assertThat(resolver.resolve(DatabaseType.MYSQL)).isInstanceOf(MySqlSqlDialect.class);
        assertThat(resolver.resolve(DatabaseType.ORACLE)).isInstanceOf(OracleSqlDialect.class);
    }
}
