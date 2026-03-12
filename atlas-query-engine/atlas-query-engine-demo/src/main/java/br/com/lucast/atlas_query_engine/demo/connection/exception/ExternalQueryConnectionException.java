package br.com.lucast.atlas_query_engine.demo.connection.exception;

import br.com.lucast.atlas_query_engine.core.exception.QueryEngineException;

public class ExternalQueryConnectionException extends QueryEngineException {

    public ExternalQueryConnectionException(String connectionKey, Throwable cause) {
        super("Failed to connect or execute query using external connection: " + connectionKey, cause);
    }
}
