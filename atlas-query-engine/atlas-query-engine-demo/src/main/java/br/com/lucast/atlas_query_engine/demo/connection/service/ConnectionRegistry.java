package br.com.lucast.atlas_query_engine.demo.connection.service;

import br.com.lucast.atlas_query_engine.demo.connection.entity.AqeConnectionEntity;
import br.com.lucast.atlas_query_engine.demo.connection.exception.ConnectionNotFoundException;
import br.com.lucast.atlas_query_engine.demo.connection.exception.InactiveConnectionException;
import br.com.lucast.atlas_query_engine.demo.connection.model.ConnectionDefinition;
import java.util.Optional;
import org.springframework.stereotype.Component;

@Component
public class ConnectionRegistry {

    private final AqeConnectionService connectionService;
    private final ConnectionCryptoService cryptoService;

    public ConnectionRegistry(AqeConnectionService connectionService, ConnectionCryptoService cryptoService) {
        this.connectionService = connectionService;
        this.cryptoService = cryptoService;
    }

    public Optional<ConnectionDefinition> findByConnectionKey(String connectionKey) {
        return connectionService.findByConnectionKey(connectionKey)
                .map(this::toDefinition);
    }

    public ConnectionDefinition resolveByConnectionKey(String connectionKey) {
        AqeConnectionEntity entity = connectionService.findByConnectionKey(connectionKey)
                .orElseThrow(() -> new ConnectionNotFoundException(connectionKey));

        if (!entity.active()) {
            throw new InactiveConnectionException(connectionKey);
        }

        return toDefinition(entity);
    }

    private ConnectionDefinition toDefinition(AqeConnectionEntity entity) {
        return new ConnectionDefinition(
                entity.connectionKey(),
                entity.dbType(),
                cryptoService.decrypt(entity.hostEncrypted()),
                Integer.parseInt(cryptoService.decrypt(entity.portEncrypted())),
                cryptoService.decrypt(entity.databaseNameEncrypted()),
                cryptoService.decrypt(entity.usernameEncrypted()),
                cryptoService.decrypt(entity.passwordEncrypted())
        );
    }
}
