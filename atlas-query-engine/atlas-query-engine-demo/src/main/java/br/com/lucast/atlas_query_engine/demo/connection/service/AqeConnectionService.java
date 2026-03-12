package br.com.lucast.atlas_query_engine.demo.connection.service;

import br.com.lucast.atlas_query_engine.demo.connection.entity.AqeConnectionEntity;
import br.com.lucast.atlas_query_engine.demo.connection.model.ConnectionRegistrationRequest;
import br.com.lucast.atlas_query_engine.demo.connection.repository.AqeConnectionRepository;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.Optional;
import org.springframework.stereotype.Service;

@Service
public class AqeConnectionService {

    private final AqeConnectionRepository repository;
    private final ConnectionCryptoService cryptoService;

    public AqeConnectionService(AqeConnectionRepository repository, ConnectionCryptoService cryptoService) {
        this.repository = repository;
        this.cryptoService = cryptoService;
    }

    public AqeConnectionEntity save(ConnectionRegistrationRequest request) {
        OffsetDateTime now = OffsetDateTime.now(ZoneOffset.UTC);
        AqeConnectionEntity entity = new AqeConnectionEntity(
                null,
                request.connectionKey(),
                request.name(),
                request.dbType(),
                cryptoService.encrypt(request.host()),
                cryptoService.encrypt(String.valueOf(request.port())),
                cryptoService.encrypt(request.databaseName()),
                cryptoService.encrypt(request.username()),
                cryptoService.encrypt(request.password()),
                request.active(),
                now,
                now
        );
        return repository.save(entity);
    }

    public Optional<AqeConnectionEntity> findByConnectionKey(String connectionKey) {
        return repository.findByConnectionKey(connectionKey);
    }
}
