package br.com.lucast.atlas_query_engine.demo.connection.exception;

import br.com.lucast.atlas_query_engine.core.exception.QueryEngineException;

public class UnsupportedDatabaseTypeException extends QueryEngineException {

    public UnsupportedDatabaseTypeException(String dbType) {
        super("Unsupported database type: " + dbType);
    }
}
