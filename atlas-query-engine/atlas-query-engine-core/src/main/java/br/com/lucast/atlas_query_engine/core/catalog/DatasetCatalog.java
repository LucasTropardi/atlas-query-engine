package br.com.lucast.atlas_query_engine.core.catalog;

import java.util.Optional;
import java.util.List;

public interface DatasetCatalog {

    List<DatasetDefinition> findAll();

    Optional<DatasetDefinition> findByName(String datasetName);
}
