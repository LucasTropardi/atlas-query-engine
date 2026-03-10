package br.com.lucast.atlas_query_engine.core.catalog;

import java.util.Optional;

public interface DatasetCatalog {

    Optional<DatasetDefinition> findByName(String datasetName);
}
