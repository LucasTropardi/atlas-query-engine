package br.com.lucast.atlas_query_engine.demo.connection.exception;

import br.com.lucast.atlas_query_engine.core.exception.QueryEngineException;

public class InactiveConnectionException extends QueryEngineException {

    public InactiveConnectionException(String connectionKey) {
        super("Connection is inactive for key: " + connectionKey);
    }
}
