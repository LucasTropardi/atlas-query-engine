package br.com.lucast.atlas_query_engine.demo.connection.exception;

import br.com.lucast.atlas_query_engine.core.exception.QueryEngineException;

public class ConnectionNotFoundException extends QueryEngineException {

    public ConnectionNotFoundException(String connectionKey) {
        super("Connection not found for key: " + connectionKey);
    }
}
