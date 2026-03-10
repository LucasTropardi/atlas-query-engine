package br.com.lucast.atlas_query_engine.core.catalog;

import br.com.lucast.atlas_query_engine.core.model.MetricOperation;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class InMemoryDatasetCatalog implements DatasetCatalog {

    private final Map<String, DatasetDefinition> datasets;

    public InMemoryDatasetCatalog() {
        SourceDefinition postgresMain = new SourceDefinition("postgres_main", "postgresql");

        DatasetDefinition customers = new DatasetDefinition(
                "customers",
                postgresMain,
                "public",
                "customers",
                Map.of(
                        "id", new DimensionDefinition("id", "customers", null, "id", FieldType.LONG, true, true),
                        "name", new DimensionDefinition("name", "customers", null, "name", FieldType.STRING, true, true)
                ),
                Map.of(),
                List.of()
        );

        DatasetDefinition orders = new DatasetDefinition(
                "orders",
                postgresMain,
                "public",
                "orders",
                Map.of(
                        "id", new DimensionDefinition("id", "orders", null, "id", FieldType.LONG, true, true),
                        "country", new DimensionDefinition("country", "orders", null, "country", FieldType.STRING, true, true),
                        "status", new DimensionDefinition("status", "orders", null, "status", FieldType.STRING, true, true),
                        "createdAt", new DimensionDefinition("createdAt", "orders", null, "created_at", FieldType.DATE, true, true),
                        "amount", new DimensionDefinition("amount", "orders", null, "amount", FieldType.DECIMAL, true, true),
                        "customerName", new DimensionDefinition("customerName", "customers", "customer", "name", FieldType.STRING, true, true)
                ),
                Map.of(
                        "id", new MetricDefinition("id", "orders", null, "id", FieldType.LONG, EnumSet.of(MetricOperation.COUNT)),
                        "amount", new MetricDefinition("amount", "orders", null, "amount", FieldType.DECIMAL,
                                EnumSet.of(MetricOperation.SUM, MetricOperation.AVG, MetricOperation.MIN, MetricOperation.MAX))
                ),
                List.of(
                        new DatasetRelationDefinition("customer", "customers", "customer_id", "id", JoinType.LEFT)
                )
        );

        this.datasets = Map.of(
                orders.getName(), orders,
                customers.getName(), customers
        );
    }

    @Override
    public Optional<DatasetDefinition> findByName(String datasetName) {
        return Optional.ofNullable(datasets.get(datasetName));
    }
}
