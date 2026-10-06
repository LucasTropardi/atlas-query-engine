package br.com.lucast.atlas_query_engine.demo.execution;

import br.com.lucast.atlas_query_engine.demo.connection.entity.DatabaseType;
import br.com.lucast.atlas_query_engine.demo.connection.exception.JdbcConnectionConfigurationException;
import br.com.lucast.atlas_query_engine.demo.connection.model.ConnectionDefinition;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DriverManagerExternalDataSourceFactoryTest {

    private final DriverManagerExternalDataSourceFactory factory = new DriverManagerExternalDataSourceFactory();

    @Test
    void shouldBuildPostgresJdbcUrl() {
        String jdbcUrl = factory.buildJdbcUrl(connectionDefinition(DatabaseType.POSTGRES));

        assertThat(jdbcUrl).isEqualTo("jdbc:postgresql://db.example:5432/analytics");
    }

    @Test
    void shouldBuildMysqlJdbcUrl() {
        String jdbcUrl = factory.buildJdbcUrl(connectionDefinition(DatabaseType.MYSQL));

        assertThat(jdbcUrl).isEqualTo("jdbc:mysql://db.example:5432/analytics");
    }

    @Test
    void shouldBuildOracleJdbcUrl() {
        String jdbcUrl = factory.buildJdbcUrl(connectionDefinition(DatabaseType.ORACLE));

        assertThat(jdbcUrl).isEqualTo("jdbc:oracle:thin:@//db.example:5432/analytics");
    }

    @Test
    void shouldFailWhenJdbcConfigurationIsInvalid() {
        ConnectionDefinition invalidDefinition = new ConnectionDefinition(
                "sales_mysql",
                DatabaseType.MYSQL,
                " ",
                3306,
                "sales",
                "user",
                "pass"
        );

        assertThatThrownBy(() -> factory.buildJdbcUrl(invalidDefinition))
                .isInstanceOf(JdbcConnectionConfigurationException.class)
                .hasMessage("Host must not be blank");
    }

    @Test
    void shouldReplaceCachedDatasourceWhenCredentialsOrDestinationChange() {
        ConnectionDefinition initial = connectionDefinition(DatabaseType.POSTGRES);
        var first = factory.create(initial);
        assertThat(factory.create(initial)).isSameAs(first);
        ConnectionDefinition changed = new ConnectionDefinition(initial.connectionKey(), DatabaseType.MYSQL,
                "new-host", 3306, "new-db", "new-user", "new-password");
        var replacement = (org.springframework.jdbc.datasource.DriverManagerDataSource) factory.create(changed);
        assertThat(replacement).isNotSameAs(first);
        assertThat(replacement.getUrl()).isEqualTo("jdbc:mysql://new-host:3306/new-db");
        assertThat(replacement.getUsername()).isEqualTo("new-user");
        assertThat(replacement.getPassword()).isEqualTo("new-password");
        assertThat(factory.create(changed)).isSameAs(replacement);
    }

    private ConnectionDefinition connectionDefinition(DatabaseType databaseType) {
        return new ConnectionDefinition(
                "analytics_pg",
                databaseType,
                "db.example",
                5432,
                "analytics",
                "analytics_user",
                "analytics_pass"
        );
    }
}
