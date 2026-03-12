package br.com.lucast.atlas_query_engine.demo.execution;

import br.com.lucast.atlas_query_engine.demo.connection.entity.DatabaseType;
import javax.sql.DataSource;

public record QueryExecutionContext(
        DataSource dataSource,
        boolean external,
        String connectionKey,
        DatabaseType dbType
) {

    public static QueryExecutionContext defaultDataSource(DataSource dataSource) {
        return new QueryExecutionContext(dataSource, false, null, null);
    }

    public static QueryExecutionContext externalDataSource(
            DataSource dataSource,
            String connectionKey,
            DatabaseType dbType
    ) {
        return new QueryExecutionContext(dataSource, true, connectionKey, dbType);
    }
}
