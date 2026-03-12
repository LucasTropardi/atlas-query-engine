package br.com.lucast.atlas_query_engine.demo.connection.service;

import br.com.lucast.atlas_query_engine.demo.config.AtlasSecurityProperties;
import br.com.lucast.atlas_query_engine.demo.connection.entity.AqeConnectionEntity;
import br.com.lucast.atlas_query_engine.demo.connection.entity.DatabaseType;
import br.com.lucast.atlas_query_engine.demo.connection.model.ConnectionDefinition;
import br.com.lucast.atlas_query_engine.demo.connection.model.ConnectionRegistrationRequest;
import br.com.lucast.atlas_query_engine.demo.connection.repository.JdbcAqeConnectionRepository;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.core.io.ClassPathResource;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabase;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseBuilder;
import org.springframework.jdbc.datasource.embedded.EmbeddedDatabaseType;
import org.springframework.jdbc.datasource.init.ResourceDatabasePopulator;

import static org.assertj.core.api.Assertions.assertThat;

class ConnectionCatalogPersistenceTest {

    private EmbeddedDatabase database;
    private JdbcTemplate jdbcTemplate;
    private AqeConnectionService connectionService;
    private ConnectionRegistry connectionRegistry;

    @BeforeEach
    void setUp() {
        database = new EmbeddedDatabaseBuilder()
                .setType(EmbeddedDatabaseType.H2)
                .generateUniqueName(true)
                .build();

        ResourceDatabasePopulator populator = new ResourceDatabasePopulator(
                new ClassPathResource("db/migration/V2__create_aqe_connection.sql")
        );
        populator.execute(database);

        jdbcTemplate = new JdbcTemplate(database);

        AtlasSecurityProperties properties = new AtlasSecurityProperties();
        properties.setEncryptionKey("test-encryption-key");

        AesConnectionCryptoService cryptoService = new AesConnectionCryptoService(properties);
        cryptoService.initialize();

        JdbcAqeConnectionRepository repository = new JdbcAqeConnectionRepository(jdbcTemplate);
        connectionService = new AqeConnectionService(repository, cryptoService);
        connectionRegistry = new ConnectionRegistry(connectionService, cryptoService);
    }

    @AfterEach
    void tearDown() {
        if (database != null) {
            database.shutdown();
        }
    }

    @Test
    void shouldPersistAndRecoverConnectionByConnectionKey() {
        ConnectionRegistrationRequest request = new ConnectionRegistrationRequest(
                "sales_mysql",
                "Sales MySQL",
                DatabaseType.MYSQL,
                "sales-db.internal",
                3306,
                "sales",
                "sales_user",
                "sales_pass",
                true
        );

        AqeConnectionEntity saved = connectionService.save(request);

        assertThat(saved.id()).isNotNull();

        AqeConnectionEntity stored = connectionService.findByConnectionKey("sales_mysql").orElseThrow();

        assertThat(stored.connectionKey()).isEqualTo("sales_mysql");
        assertThat(stored.dbType()).isEqualTo(DatabaseType.MYSQL);
        assertThat(stored.active()).isTrue();
    }

    @Test
    void shouldStoreSensitiveFieldsEncryptedAndDecryptThemThroughRegistry() {
        connectionService.save(new ConnectionRegistrationRequest(
                "analytics_pg",
                "Analytics PostgreSQL",
                DatabaseType.POSTGRES,
                "analytics-db.internal",
                5432,
                "analytics",
                "analytics_user",
                "analytics_pass",
                true
        ));

        Map<String, Object> rawRow = jdbcTemplate.queryForMap(
                """
                SELECT host_encrypted, port_encrypted, database_name_encrypted, username_encrypted, password_encrypted
                FROM aqe_connection
                WHERE connection_key = ?
                """,
                "analytics_pg"
        );

        assertThat(rawRow.get("host_encrypted")).isNotEqualTo("analytics-db.internal");
        assertThat(rawRow.get("port_encrypted")).isNotEqualTo("5432");
        assertThat(rawRow.get("database_name_encrypted")).isNotEqualTo("analytics");
        assertThat(rawRow.get("username_encrypted")).isNotEqualTo("analytics_user");
        assertThat(rawRow.get("password_encrypted")).isNotEqualTo("analytics_pass");

        ConnectionDefinition definition = connectionRegistry.findByConnectionKey("analytics_pg").orElseThrow();

        assertThat(definition.connectionKey()).isEqualTo("analytics_pg");
        assertThat(definition.dbType()).isEqualTo(DatabaseType.POSTGRES);
        assertThat(definition.host()).isEqualTo("analytics-db.internal");
        assertThat(definition.port()).isEqualTo(5432);
        assertThat(definition.databaseName()).isEqualTo("analytics");
        assertThat(definition.username()).isEqualTo("analytics_user");
        assertThat(definition.password()).isEqualTo("analytics_pass");
    }
}
