package br.com.lucast.atlas_query_engine.core.exception;

public class QueryEngineException extends RuntimeException {

    public QueryEngineException(String message) {
        super(message);
    }

    public QueryEngineException(String message, Throwable cause) {
        super(message, cause);
    }
}
