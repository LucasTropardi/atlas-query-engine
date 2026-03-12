package br.com.lucast.atlas_query_engine.demo.controller;

import br.com.lucast.atlas_query_engine.demo.connection.entity.AqeConnectionEntity;
import br.com.lucast.atlas_query_engine.demo.connection.model.ConnectionRegistrationRequest;
import br.com.lucast.atlas_query_engine.demo.connection.service.AqeConnectionService;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/internal/connections")
@ConditionalOnProperty(prefix = "atlas.dev.internal-connections", name = "enabled", havingValue = "true")
public class InternalConnectionController {

    private final AqeConnectionService connectionService;

    public InternalConnectionController(AqeConnectionService connectionService) {
        this.connectionService = connectionService;
    }

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    public InternalConnectionResponse register(@RequestBody ConnectionRegistrationRequest request) {
        AqeConnectionEntity saved = connectionService.save(request);
        return new InternalConnectionResponse(
                saved.id(),
                saved.connectionKey(),
                saved.name(),
                saved.dbType().name(),
                saved.active()
        );
    }

    public record InternalConnectionResponse(
            Long id,
            String connectionKey,
            String name,
            String dbType,
            boolean active
    ) {
    }
}
