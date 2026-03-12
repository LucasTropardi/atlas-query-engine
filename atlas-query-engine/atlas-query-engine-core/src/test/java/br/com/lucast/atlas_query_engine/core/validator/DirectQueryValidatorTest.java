package br.com.lucast.atlas_query_engine.core.validator;

import br.com.lucast.atlas_query_engine.core.model.ColumnExpression;
import br.com.lucast.atlas_query_engine.core.model.ExistsFilterRequest;
import br.com.lucast.atlas_query_engine.core.catalog.InMemoryDatasetCatalog;
import br.com.lucast.atlas_query_engine.core.exception.InvalidQueryException;
import br.com.lucast.atlas_query_engine.core.model.FilterOperator;
import br.com.lucast.atlas_query_engine.core.model.FilterRequest;
import br.com.lucast.atlas_query_engine.core.model.FunctionExpression;
import br.com.lucast.atlas_query_engine.core.model.JoinRequest;
import br.com.lucast.atlas_query_engine.core.model.ProjectionRequest;
import br.com.lucast.atlas_query_engine.core.model.QueryRequest;
import br.com.lucast.atlas_query_engine.core.parser.QueryParser;
import jakarta.validation.Validation;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThatThrownBy;

class DirectQueryValidatorTest {

    private final QueryParser parser = new QueryParser();
    private final QueryValidator validator = new QueryValidator(
            new InMemoryDatasetCatalog(),
            Validation.buildDefaultValidatorFactory().getValidator()
    );

    @Test
    void shouldAllowDirectQueryWithQualifiedFields() {
        QueryRequest request = new QueryRequest();
        request.setTable("customer_companies");
        request.setAlias("cc");
        request.setSelect(List.of("cc.id", "cc.legal_name"));
        request.setJoins(List.of(new JoinRequest("public", "customer_company_address", "cca",
                br.com.lucast.atlas_query_engine.core.model.JoinType.LEFT, "cc.id", "cca.customer_company_id")));
        request.setFilters(List.of(new FilterRequest("cc.active", FilterOperator.EQUALS, true)));

        validator.validate(parser.parse(request));
    }

    @Test
    void shouldRejectUnsafeDirectFieldReference() {
        QueryRequest request = new QueryRequest();
        request.setTable("customer_companies");
        request.setSelect(List.of("cc.id", "cc.legal_name"));
        request.setFilters(List.of(new FilterRequest("cc.id; drop table x", FilterOperator.EQUALS, 1)));

        assertThatThrownBy(() -> validator.validate(parser.parse(request)))
                .isInstanceOf(InvalidQueryException.class)
                .hasMessageContaining("Filter field is invalid");
    }

    @Test
    void shouldAllowProjectionExpressionsAndExistsFilters() {
        QueryRequest request = new QueryRequest();
        request.setTable("customer_companies");
        request.setAlias("cc");
        request.setProjections(List.of(
                new ProjectionRequest("display_name", new FunctionExpression("coalesce", List.of(
                        new ColumnExpression("cc.trade_name"),
                        new ColumnExpression("cc.legal_name")
                )))
        ));
        request.setFilterTree(new ExistsFilterRequest(
                null,
                "customer_company_address",
                "cca",
                br.com.lucast.atlas_query_engine.core.model.JoinType.INNER,
                "cc.id",
                "cca.customer_company_id",
                List.of(),
                br.com.lucast.atlas_query_engine.core.model.FilterGroupRequest.empty()
        ));

        validator.validate(parser.parse(request));
    }
}
