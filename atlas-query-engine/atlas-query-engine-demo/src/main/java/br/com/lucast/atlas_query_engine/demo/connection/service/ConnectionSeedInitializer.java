package br.com.lucast.atlas_query_engine.demo.connection.service;

import br.com.lucast.atlas_query_engine.demo.connection.entity.DatabaseType;
import br.com.lucast.atlas_query_engine.demo.connection.model.ConnectionRegistrationRequest;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

@Component
public class ConnectionSeedInitializer implements ApplicationRunner {

    private final AqeConnectionService connectionService;

    public ConnectionSeedInitializer(AqeConnectionService connectionService) {
        this.connectionService = connectionService;
    }

    @Override
    public void run(ApplicationArguments args) {
        seedIfMissing(new ConnectionRegistrationRequest(
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
        seedIfMissing(new ConnectionRegistrationRequest(
                "sales_mysql",
                "Sales MySQL",
                DatabaseType.MYSQL,
                "sales-db.internal",
                3306,
                "sales",
                "sales_user",
                "sales_pass",
                true
        ));
    }

    private void seedIfMissing(ConnectionRegistrationRequest request) {
        if (connectionService.findByConnectionKey(request.connectionKey()).isEmpty()) {
            connectionService.save(request);
        }
    }
}
