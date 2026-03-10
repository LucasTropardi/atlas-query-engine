package br.com.lucast.atlas_query_engine.core.catalog;

import java.util.Map;
import java.util.Optional;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class DatasetDefinition {

    private final String name;
    private final SourceDefinition source;
    private final String schemaName;
    private final String tableName;
    private final Map<String, DimensionDefinition> dimensions;
    private final Map<String, MetricDefinition> metrics;
    private final List<DatasetRelationDefinition> relations;

    public Optional<DimensionDefinition> findDimension(String logicalName) {
        return Optional.ofNullable(dimensions.get(logicalName));
    }

    public Optional<MetricDefinition> findMetric(String logicalField) {
        return Optional.ofNullable(metrics.get(logicalField));
    }

    public String getQualifiedTableName() {
        return schemaName + "." + tableName;
    }

    public Optional<DatasetRelationDefinition> findRelation(String relationName) {
        return relations.stream()
                .filter(relation -> relation.getRelationName().equals(relationName))
                .findFirst();
    }
}
