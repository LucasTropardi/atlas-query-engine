package br.com.lucast.atlas_query_engine.demo.config;

import br.com.lucast.atlas_query_engine.demo.connection.entity.DatabaseType;
import jakarta.validation.constraints.NotNull;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "atlas.sql")
public class AtlasSqlProperties {

    @NotNull
    private DatabaseType defaultDialect = DatabaseType.POSTGRES;

    public DatabaseType getDefaultDialect() {
        return defaultDialect;
    }

    public void setDefaultDialect(DatabaseType defaultDialect) {
        this.defaultDialect = defaultDialect;
    }
}
