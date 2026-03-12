package br.com.lucast.atlas_query_engine.demo.connection.repository;

import br.com.lucast.atlas_query_engine.demo.connection.entity.AqeConnectionEntity;
import br.com.lucast.atlas_query_engine.demo.connection.entity.DatabaseType;
import java.sql.PreparedStatement;
import java.sql.Timestamp;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Optional;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.support.GeneratedKeyHolder;
import org.springframework.jdbc.support.KeyHolder;
import org.springframework.dao.support.DataAccessUtils;
import org.springframework.stereotype.Repository;

@Repository
public class JdbcAqeConnectionRepository implements AqeConnectionRepository {

    private static final String INSERT_SQL = """
            INSERT INTO aqe_connection (
                connection_key,
                name,
                db_type,
                host_encrypted,
                port_encrypted,
                database_name_encrypted,
                username_encrypted,
                password_encrypted,
                active,
                created_at,
                updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """;

    private static final String FIND_BY_CONNECTION_KEY_SQL = """
            SELECT
                id,
                connection_key,
                name,
                db_type,
                host_encrypted,
                port_encrypted,
                database_name_encrypted,
                username_encrypted,
                password_encrypted,
                active,
                created_at,
                updated_at
            FROM aqe_connection
            WHERE connection_key = ?
            """;

    private final JdbcTemplate jdbcTemplate;

    public JdbcAqeConnectionRepository(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public AqeConnectionEntity save(AqeConnectionEntity entity) {
        KeyHolder keyHolder = new GeneratedKeyHolder();
        jdbcTemplate.update(connection -> {
            PreparedStatement statement = connection.prepareStatement(INSERT_SQL, new String[]{"id"});
            statement.setString(1, entity.connectionKey());
            statement.setString(2, entity.name());
            statement.setString(3, entity.dbType().name());
            statement.setString(4, entity.hostEncrypted());
            statement.setString(5, entity.portEncrypted());
            statement.setString(6, entity.databaseNameEncrypted());
            statement.setString(7, entity.usernameEncrypted());
            statement.setString(8, entity.passwordEncrypted());
            statement.setBoolean(9, entity.active());
            statement.setTimestamp(10, Timestamp.from(entity.createdAt().toInstant()));
            statement.setTimestamp(11, Timestamp.from(entity.updatedAt().toInstant()));
            return statement;
        }, keyHolder);

        Number key = keyHolder.getKey();
        Long id = key == null ? null : key.longValue();
        return new AqeConnectionEntity(
                id,
                entity.connectionKey(),
                entity.name(),
                entity.dbType(),
                entity.hostEncrypted(),
                entity.portEncrypted(),
                entity.databaseNameEncrypted(),
                entity.usernameEncrypted(),
                entity.passwordEncrypted(),
                entity.active(),
                entity.createdAt(),
                entity.updatedAt()
        );
    }

    @Override
    public Optional<AqeConnectionEntity> findByConnectionKey(String connectionKey) {
        List<AqeConnectionEntity> results = jdbcTemplate.query(
                FIND_BY_CONNECTION_KEY_SQL,
                (rs, rowNum) -> new AqeConnectionEntity(
                        rs.getLong("id"),
                        rs.getString("connection_key"),
                        rs.getString("name"),
                        DatabaseType.valueOf(rs.getString("db_type")),
                        rs.getString("host_encrypted"),
                        rs.getString("port_encrypted"),
                        rs.getString("database_name_encrypted"),
                        rs.getString("username_encrypted"),
                        rs.getString("password_encrypted"),
                        rs.getBoolean("active"),
                        toOffsetDateTime(rs.getTimestamp("created_at")),
                        toOffsetDateTime(rs.getTimestamp("updated_at"))
                ),
                connectionKey
        );
        return Optional.ofNullable(DataAccessUtils.singleResult(results));
    }

    private OffsetDateTime toOffsetDateTime(Timestamp timestamp) {
        return timestamp == null ? null : timestamp.toInstant().atOffset(ZoneOffset.UTC);
    }
}
