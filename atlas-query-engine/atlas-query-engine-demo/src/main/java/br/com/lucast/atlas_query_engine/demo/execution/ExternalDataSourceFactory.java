package br.com.lucast.atlas_query_engine.demo.execution;

import br.com.lucast.atlas_query_engine.demo.connection.model.ConnectionDefinition;
import javax.sql.DataSource;

public interface ExternalDataSourceFactory {

    DataSource create(ConnectionDefinition connectionDefinition);

    String buildJdbcUrl(ConnectionDefinition connectionDefinition);
}
