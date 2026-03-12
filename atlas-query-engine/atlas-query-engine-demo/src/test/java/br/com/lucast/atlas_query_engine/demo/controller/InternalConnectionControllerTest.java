package br.com.lucast.atlas_query_engine.demo.controller;

import br.com.lucast.atlas_query_engine.demo.config.AtlasSecurityProperties;
import br.com.lucast.atlas_query_engine.demo.connection.entity.DatabaseType;
import br.com.lucast.atlas_query_engine.demo.connection.model.ConnectionRegistrationRequest;
import br.com.lucast.atlas_query_engine.demo.connection.repository.JdbcAqeConnectionRepository;
import br.com.lucast.atlas_query_engine.demo.connection.service.AesConnectionCryptoService;
import br.com.lucast.atlas_query_engine.demo.connection.service.AqeConnectionService;
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

class InternalConnectionControllerTest {

    private EmbeddedDatabase database;
    private JdbcTemplate jdbcTemplate;
    private InternalConnectionController controller;

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

        AqeConnectionService connectionService = new AqeConnectionService(
                new JdbcAqeConnectionRepository(jdbcTemplate),
                cryptoService
        );
        controller = new InternalConnectionController(connectionService);
    }

    @AfterEach
    void tearDown() {
        if (database != null) {
            database.shutdown();
        }
    }

    @Test
    void shouldRegisterConnectionThroughInternalControllerUsingEncryptedPersistence() {
        InternalConnectionController.InternalConnectionResponse response = controller.register(
                new ConnectionRegistrationRequest(
                        "vetcare_pg",
                        "VetCare PostgreSQL",
                        DatabaseType.POSTGRES,
                        "vetcare-db.internal",
                        5432,
                        "vetcare",
                        "vetcare_user",
                        "vetcare_pass",
                        true
                )
        );

        assertThat(response.connectionKey()).isEqualTo("vetcare_pg");
        assertThat(response.dbType()).isEqualTo("POSTGRES");

        Map<String, Object> rawRow = jdbcTemplate.queryForMap(
                "SELECT host_encrypted, password_encrypted FROM aqe_connection WHERE connection_key = ?",
                "vetcare_pg"
        );

        assertThat(rawRow.get("host_encrypted")).isNotEqualTo("vetcare-db.internal");
        assertThat(rawRow.get("password_encrypted")).isNotEqualTo("vetcare_pass");
    }
}
