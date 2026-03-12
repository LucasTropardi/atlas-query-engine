package br.com.lucast.atlas_query_engine.core.catalog;

import br.com.lucast.atlas_query_engine.core.model.MetricOperation;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static java.util.Map.entry;

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

        DatasetDefinition customerCompanyAddress = new DatasetDefinition(
                "customerCompanyAddress",
                postgresMain,
                "public",
                "customer_company_address",
                Map.ofEntries(
                        entry("customerCompanyId", new DimensionDefinition("customerCompanyId", "customerCompanyAddress", null,
                                "customer_company_id", FieldType.LONG, true, true)),
                        entry("zipCode", new DimensionDefinition("zipCode", "customerCompanyAddress", null,
                                "zip_code", FieldType.STRING, true, true)),
                        entry("street", new DimensionDefinition("street", "customerCompanyAddress", null,
                                "street", FieldType.STRING, true, true)),
                        entry("number", new DimensionDefinition("number", "customerCompanyAddress", null,
                                "number", FieldType.STRING, true, true)),
                        entry("complement", new DimensionDefinition("complement", "customerCompanyAddress", null,
                                "complement", FieldType.STRING, true, true)),
                        entry("neighborhood", new DimensionDefinition("neighborhood", "customerCompanyAddress", null,
                                "neighborhood", FieldType.STRING, true, true)),
                        entry("cityName", new DimensionDefinition("cityName", "customerCompanyAddress", null,
                                "city_name", FieldType.STRING, true, true)),
                        entry("cityIbge", new DimensionDefinition("cityIbge", "customerCompanyAddress", null,
                                "city_ibge", FieldType.STRING, true, true)),
                        entry("stateUf", new DimensionDefinition("stateUf", "customerCompanyAddress", null,
                                "state_uf", FieldType.STRING, true, true)),
                        entry("country", new DimensionDefinition("country", "customerCompanyAddress", null,
                                "country", FieldType.STRING, true, true)),
                        entry("createdAt", new DimensionDefinition("createdAt", "customerCompanyAddress", null,
                                "created_at", FieldType.DATE, true, true)),
                        entry("updatedAt", new DimensionDefinition("updatedAt", "customerCompanyAddress", null,
                                "updated_at", FieldType.DATE, true, true))
                ),
                Map.of(),
                List.of()
        );

        DatasetDefinition customerCompanies = new DatasetDefinition(
                "customerCompanies",
                postgresMain,
                "public",
                "customer_companies",
                Map.ofEntries(
                        entry("id", new DimensionDefinition("id", "customerCompanies", null, "id", FieldType.LONG, true, true)),
                        entry("tutorId", new DimensionDefinition("tutorId", "customerCompanies", null,
                                "tutor_id", FieldType.LONG, true, true)),
                        entry("legalName", new DimensionDefinition("legalName", "customerCompanies", null,
                                "legal_name", FieldType.STRING, true, true)),
                        entry("tradeName", new DimensionDefinition("tradeName", "customerCompanies", null,
                                "trade_name", FieldType.STRING, true, true)),
                        entry("cnpj", new DimensionDefinition("cnpj", "customerCompanies", null,
                                "cnpj", FieldType.STRING, true, true)),
                        entry("phone", new DimensionDefinition("phone", "customerCompanies", null,
                                "phone", FieldType.STRING, true, true)),
                        entry("email", new DimensionDefinition("email", "customerCompanies", null,
                                "email", FieldType.STRING, true, true)),
                        entry("active", new DimensionDefinition("active", "customerCompanies", null,
                                "active", FieldType.BOOLEAN, true, true)),
                        entry("createdAt", new DimensionDefinition("createdAt", "customerCompanies", null,
                                "created_at", FieldType.DATE, true, true)),
                        entry("updatedAt", new DimensionDefinition("updatedAt", "customerCompanies", null,
                                "updated_at", FieldType.DATE, true, true)),
                        entry("zipCode", new DimensionDefinition("zipCode", "customerCompanyAddress", "address",
                                "zip_code", FieldType.STRING, true, true)),
                        entry("street", new DimensionDefinition("street", "customerCompanyAddress", "address",
                                "street", FieldType.STRING, true, true)),
                        entry("number", new DimensionDefinition("number", "customerCompanyAddress", "address",
                                "number", FieldType.STRING, true, true)),
                        entry("complement", new DimensionDefinition("complement", "customerCompanyAddress", "address",
                                "complement", FieldType.STRING, true, true)),
                        entry("neighborhood", new DimensionDefinition("neighborhood", "customerCompanyAddress", "address",
                                "neighborhood", FieldType.STRING, true, true)),
                        entry("cityName", new DimensionDefinition("cityName", "customerCompanyAddress", "address",
                                "city_name", FieldType.STRING, true, true)),
                        entry("cityIbge", new DimensionDefinition("cityIbge", "customerCompanyAddress", "address",
                                "city_ibge", FieldType.STRING, true, true)),
                        entry("stateUf", new DimensionDefinition("stateUf", "customerCompanyAddress", "address",
                                "state_uf", FieldType.STRING, true, true)),
                        entry("country", new DimensionDefinition("country", "customerCompanyAddress", "address",
                                "country", FieldType.STRING, true, true))
                ),
                Map.of(),
                List.of(
                        new DatasetRelationDefinition("address", "customerCompanyAddress", "id", "customer_company_id", JoinType.LEFT)
                )
        );

        this.datasets = Map.of(
                orders.getName(), orders,
                customers.getName(), customers,
                customerCompanyAddress.getName(), customerCompanyAddress,
                customerCompanies.getName(), customerCompanies
        );
    }

    @Override
    public Optional<DatasetDefinition> findByName(String datasetName) {
        return Optional.ofNullable(datasets.get(datasetName));
    }
}
