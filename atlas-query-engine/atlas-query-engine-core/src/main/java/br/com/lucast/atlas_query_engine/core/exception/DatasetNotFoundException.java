package br.com.lucast.atlas_query_engine.core.exception;

public class DatasetNotFoundException extends QueryEngineException {

    public DatasetNotFoundException(String datasetName) {
        super("Dataset not found: " + datasetName);
    }
}
