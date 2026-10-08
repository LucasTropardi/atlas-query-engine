package br.com.lucast.atlas_query_engine.core.api;

import br.com.lucast.atlas_query_engine.core.catalog.InMemoryDatasetCatalog;
import br.com.lucast.atlas_query_engine.core.exception.InvalidQueryException;
import br.com.lucast.atlas_query_engine.core.model.*;
import br.com.lucast.atlas_query_engine.core.parser.QueryParser;
import br.com.lucast.atlas_query_engine.core.planner.ExecutionPlanner;
import br.com.lucast.atlas_query_engine.core.support.TestDatasets;
import br.com.lucast.atlas_query_engine.core.translator.*;
import br.com.lucast.atlas_query_engine.core.validator.QueryValidator;
import jakarta.validation.Validation;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;
import static org.assertj.core.api.Assertions.*;

class QueryPreviewTest {
    @Test
    void shouldPrepareIdenticalSqlWithoutExecutingOrDisclosingValuesAcrossModesAndDialects() {
        try (var factory = Validation.buildDefaultValidatorFactory()) {
            var catalog = new InMemoryDatasetCatalog(TestDatasets.definitions());
            for (SqlDialect dialect : List.of(new PostgresSqlDialect(), new MySqlSqlDialect(), new OracleSqlDialect())) {
                for (boolean direct : List.of(false, true)) {
                    AtomicReference<SqlQuery> executed = new AtomicReference<>();
                    var engine = new DefaultQueryEngine(new QueryParser(), new QueryValidator(catalog, factory.getValidator()),
                            new ExecutionPlanner(catalog), new SqlTranslator(), new StaticSqlDialectResolver(dialect),
                            (request, sql) -> { executed.set(sql); return null; });
                    QueryRequest request = new QueryRequest();
                    if (direct) request.setTable("orders"); else request.setDataset("orders");
                    request.setSelect(List.of("country"));
                    request.setFilters(List.of(new FilterRequest("status", FilterOperator.EQUALS, "secret-marker"),
                            new FilterRequest("country", FilterOperator.IS_NOT_NULL, null)));
                    var preview = engine.preview(request);
                    assertThat(executed.get()).isNull();
                    assertThat(preview.dialect()).isEqualTo(dialect.dialectName());
                    assertThat(preview.fields()).containsExactly("country");
                    assertThat(preview.parameters()).hasSize(1);
                    assertThat(preview.parameters().getFirst().position()).isEqualTo(1);
                    assertThat(preview.parameters().getFirst().type()).isEqualTo("String");
                    assertThat(new ObjectMapper().writeValueAsString(preview)).doesNotContain("secret-marker");
                    assertThat(preview.sql()).contains("IS NOT NULL");
                    engine.execute(request);
                    assertThat(executed.get().getSql()).isEqualTo(preview.sql());
                    assertThat(executed.get().getParameters()).containsExactly("secret-marker");
                }
            }
        }
    }

    @Test
    void shouldIncludeResolvedDatasetRelationsAndRejectInvalidPreview() {
        try (var factory = Validation.buildDefaultValidatorFactory()) {
            var catalog = new InMemoryDatasetCatalog(TestDatasets.definitions());
            var engine = new DefaultQueryEngine(new QueryParser(), new QueryValidator(catalog, factory.getValidator()),
                    new ExecutionPlanner(catalog), new SqlTranslator(), new StaticSqlDialectResolver(new PostgresSqlDialect()),
                    (request, sql) -> { throw new AssertionError("Preview must not execute SQL"); });
            var request = new QueryRequest();
            request.setDataset("orders");
            request.setSelect(List.of("customerName"));
            request.setFilters(List.of(new FilterRequest("customerName", FilterOperator.IS_NULL, null)));
            assertThat(engine.preview(request).relations()).containsExactly("customer");
            assertThat(engine.preview(request).parameters()).isEmpty();
            request.setTable("orders");
            assertThatThrownBy(() -> engine.preview(request)).isInstanceOf(InvalidQueryException.class);
        }
    }
}
