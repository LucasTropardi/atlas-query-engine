package br.com.lucast.atlas_query_engine.demo.connection.entity;

import java.time.OffsetDateTime;

public record AqeConnectionEntity(
        Long id,
        String connectionKey,
        String name,
        DatabaseType dbType,
        String hostEncrypted,
        String portEncrypted,
        String databaseNameEncrypted,
        String usernameEncrypted,
        String passwordEncrypted,
        boolean active,
        OffsetDateTime createdAt,
        OffsetDateTime updatedAt
) {
}
