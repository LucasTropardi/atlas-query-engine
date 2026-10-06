package br.com.lucast.atlas_query_engine.core.catalog;

import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class InMemoryDatasetCatalog implements DatasetCatalog {
    private final Map<String, DatasetDefinition> datasets;

    public InMemoryDatasetCatalog() {
        this(List.of());
    }

    public InMemoryDatasetCatalog(Collection<DatasetDefinition> definitions) {
        Map<String, DatasetDefinition> indexed = new LinkedHashMap<>();
        for (DatasetDefinition definition : definitions) {
            if (indexed.putIfAbsent(definition.getName(), definition) != null) {
                throw new IllegalArgumentException("Duplicate dataset: " + definition.getName());
            }
        }
        datasets = Map.copyOf(indexed);
    }

    @Override
    public Optional<DatasetDefinition> findByName(String datasetName) {
        return Optional.ofNullable(datasets.get(datasetName));
    }
}
