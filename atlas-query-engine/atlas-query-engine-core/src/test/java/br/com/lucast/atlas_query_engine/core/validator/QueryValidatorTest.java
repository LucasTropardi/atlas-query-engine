package br.com.lucast.atlas_query_engine.core.validator;

import br.com.lucast.atlas_query_engine.core.model.ProjectionRequest;
import br.com.lucast.atlas_query_engine.core.model.JoinType;
import br.com.lucast.atlas_query_engine.core.model.ExistsFilterRequest;
import br.com.lucast.atlas_query_engine.core.model.ColumnExpression;
import br.com.lucast.atlas_query_engine.core.support.TestDatasets;
import br.com.lucast.atlas_query_engine.core.catalog.InMemoryDatasetCatalog;
import br.com.lucast.atlas_query_engine.core.exception.DatasetNotFoundException;
import br.com.lucast.atlas_query_engine.core.exception.FieldNotAllowedException;
import br.com.lucast.atlas_query_engine.core.exception.InvalidQueryException;
import br.com.lucast.atlas_query_engine.core.model.FilterGroupRequest;
import br.com.lucast.atlas_query_engine.core.model.LogicalOperator;
import br.com.lucast.atlas_query_engine.core.model.FilterOperator;
import br.com.lucast.atlas_query_engine.core.model.FilterRequest;
import br.com.lucast.atlas_query_engine.core.model.MetricOperation;
import br.com.lucast.atlas_query_engine.core.model.MetricRequest;
import br.com.lucast.atlas_query_engine.core.model.QueryRequest;
import br.com.lucast.atlas_query_engine.core.parser.QueryParser;
import jakarta.validation.Validation;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class QueryValidatorTest {

    private final QueryParser parser = new QueryParser();
    private final QueryValidator validator = new QueryValidator(
            new InMemoryDatasetCatalog(TestDatasets.definitions()),
            Validation.buildDefaultValidatorFactory().getValidator()
    );

    @Test
    void shouldRejectUnknownDataset() {
        QueryRequest request = new QueryRequest();
        request.setDataset("missing");
        request.getSelect().add("country");

        assertThatThrownBy(() -> validator.validate(parser.parse(request)))
                .isInstanceOf(DatasetNotFoundException.class);
    }

    @Test
    void shouldRequireGroupByForSelectedDimensionsWhenMetricsExist() {
        QueryRequest request = new QueryRequest();
        request.setDataset("orders");
        request.getSelect().add("country");
        request.getMetrics().add(new MetricRequest("amount", MetricOperation.SUM, "totalAmount"));

        assertThatThrownBy(() -> validator.validate(parser.parse(request)))
                .isInstanceOf(InvalidQueryException.class)
                .hasMessageContaining("groupBy");
    }

    @Test
    void shouldRejectInvalidBetweenFilterPayload() {
        QueryRequest request = new QueryRequest();
        request.setDataset("orders");
        request.getSelect().add("country");
        request.setFilters(List.of(new FilterRequest("createdAt", FilterOperator.BETWEEN, "2026-01-01")));

        assertThatThrownBy(() -> validator.validate(parser.parse(request)))
                .isInstanceOf(InvalidQueryException.class)
                .hasMessageContaining("BETWEEN");
    }

    @Test
    void shouldAllowFieldFromRelatedDatasetWhenRelationExists() {
        QueryRequest request = new QueryRequest();
        request.setDataset("orders");
        request.setSelect(List.of("country", "customerName"));
        request.setMetrics(List.of(new MetricRequest("amount", MetricOperation.SUM, "revenue")));
        request.setGroupBy(List.of("country", "customerName"));

        validator.validate(parser.parse(request));
    }

    @Test
    void shouldValidateNestedFiltersAgainstRelatedDatasetDimensions() {
        QueryRequest request = new QueryRequest();
        request.setDataset("orders");
        request.setSelect(List.of("country"));
        request.setMetrics(List.of(new MetricRequest("amount", MetricOperation.SUM, "revenue")));
        request.setGroupBy(List.of("country"));
        request.setFilterTree(new FilterGroupRequest(
                LogicalOperator.AND,
                List.of(
                        new FilterRequest("status", FilterOperator.EQUALS, "PAID"),
                        new FilterGroupRequest(
                                LogicalOperator.OR,
                                List.of(
                                        new FilterRequest("customerName", FilterOperator.LIKE, "Ana%"),
                                        new FilterRequest("country", FilterOperator.EQUALS, "BR")
                                )
                        )
                )
        ));

        validator.validate(parser.parse(request));
    }

    @Test
    void shouldRejectFieldThatDoesNotBelongToBaseOrRelatedDataset() {
        QueryRequest request = new QueryRequest();
        request.setDataset("orders");
        request.setSelect(List.of("name"));

        assertThatThrownBy(() -> validator.validate(parser.parse(request)))
                .isInstanceOf(FieldNotAllowedException.class)
                .hasMessageContaining("Selected field does not exist");
    }
    @Test
    void shouldRejectDirectFeaturesInDatasetQueries() {
        QueryRequest request = new QueryRequest();
        request.setDataset("orders");
        request.setProjections(List.of(new ProjectionRequest(
                "country", new ColumnExpression("country"))));
        assertThatThrownBy(() -> validator.validate(parser.parse(request)))
                .isInstanceOf(InvalidQueryException.class).hasMessageContaining("Dataset queries do not support");
    }

    @Test
    void shouldRejectExistsInDatasetQueriesRatherThanIgnoreIt() {
        QueryRequest request = new QueryRequest();
        request.setDataset("orders");
        request.setSelect(List.of("country"));
        request.setFilterTree(new ExistsFilterRequest(
                null, "customers", null, JoinType.INNER,
                "orders.customer_id", "customers.id", List.of(), FilterGroupRequest.empty()));
        assertThatThrownBy(() -> validator.validate(parser.parse(request)))
                .isInstanceOf(InvalidQueryException.class).hasMessageContaining("Unsupported filter node for dataset");
    }

}
