package br.com.lucast.atlas_query_engine.demo.connection.exception;

import br.com.lucast.atlas_query_engine.core.exception.QueryEngineException;

public class JdbcConnectionConfigurationException extends QueryEngineException {

    public JdbcConnectionConfigurationException(String message) {
        super(message);
    }
}
