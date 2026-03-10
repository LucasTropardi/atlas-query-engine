package br.com.lucast.atlas_query_engine.core.validator;

import br.com.lucast.atlas_query_engine.core.catalog.InMemoryDatasetCatalog;
import br.com.lucast.atlas_query_engine.core.exception.DatasetNotFoundException;
import br.com.lucast.atlas_query_engine.core.exception.FieldNotAllowedException;
import br.com.lucast.atlas_query_engine.core.exception.InvalidQueryException;
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
            new InMemoryDatasetCatalog(),
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
        request.getFilters().add(new FilterRequest("createdAt", FilterOperator.BETWEEN, "2026-01-01"));

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
    void shouldRejectFieldThatDoesNotBelongToBaseOrRelatedDataset() {
        QueryRequest request = new QueryRequest();
        request.setDataset("orders");
        request.setSelect(List.of("name"));

        assertThatThrownBy(() -> validator.validate(parser.parse(request)))
                .isInstanceOf(FieldNotAllowedException.class)
                .hasMessageContaining("Selected field does not exist");
    }
}
