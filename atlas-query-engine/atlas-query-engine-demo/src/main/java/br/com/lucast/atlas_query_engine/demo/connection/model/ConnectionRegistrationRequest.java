package br.com.lucast.atlas_query_engine.demo.connection.model;

import br.com.lucast.atlas_query_engine.demo.connection.entity.DatabaseType;

public record ConnectionRegistrationRequest(
        String connectionKey,
        String name,
        DatabaseType dbType,
        String host,
        int port,
        String databaseName,
        String username,
        String password,
        boolean active
) {
}
