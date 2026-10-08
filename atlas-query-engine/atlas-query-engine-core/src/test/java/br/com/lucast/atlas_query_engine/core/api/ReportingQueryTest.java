package br.com.lucast.atlas_query_engine.core.api;

import br.com.lucast.atlas_query_engine.core.catalog.*;
import br.com.lucast.atlas_query_engine.core.exception.InvalidQueryException;
import br.com.lucast.atlas_query_engine.core.executor.JdbcQueryExecutor;
import br.com.lucast.atlas_query_engine.core.model.*;
import br.com.lucast.atlas_query_engine.core.parser.QueryParser;
import br.com.lucast.atlas_query_engine.core.planner.ExecutionPlanner;
import br.com.lucast.atlas_query_engine.core.translator.*;
import br.com.lucast.atlas_query_engine.core.validator.QueryValidator;
import jakarta.validation.Validation;
import jakarta.validation.ValidatorFactory;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.embedded.*;
import tools.jackson.databind.ObjectMapper;
import static org.assertj.core.api.Assertions.*;

class ReportingQueryTest {
    private EmbeddedDatabase database;
    private ValidatorFactory validators;
    private InMemoryDatasetCatalog catalog;
    private DefaultQueryEngine engine;

    @BeforeEach
    void setUp() {
        database = new EmbeddedDatabaseBuilder().setType(EmbeddedDatabaseType.H2).generateUniqueName(true).build();
        var jdbc = new JdbcTemplate(database);
        jdbc.execute("CREATE TABLE SALES (ID INTEGER, COUNTRY VARCHAR, AMOUNT DECIMAL(10,2), STATUS VARCHAR)");
        jdbc.execute("INSERT INTO SALES VALUES (1,'BR',10,'PAID'),(2,'BR',20,'PAID'),(3,'US',5,'PAID'),"
                + "(4,'PT',50,'CANCELLED'),(5,'DE',40,'PAID'),(6,'AR',NULL,'PAID')");
        validators = Validation.buildDefaultValidatorFactory();
        catalog = new InMemoryDatasetCatalog(List.of(new DatasetDefinition("sales", new SourceDefinition("local", "postgresql"),
                "PUBLIC", "SALES", Map.of(
                "country", new DimensionDefinition("country", "sales", null, "COUNTRY", FieldType.STRING, true, true),
                "status", new DimensionDefinition("status", "sales", null, "STATUS", FieldType.STRING, true, true)),
                Map.of("amount", new MetricDefinition("amount", "sales", null, "AMOUNT", FieldType.DECIMAL, Set.of(MetricOperation.SUM))),
                List.of())));
        engine = new DefaultQueryEngine(new QueryParser(), new QueryValidator(catalog, validators.getValidator()),
                new ExecutionPlanner(catalog), new SqlTranslator(), new StaticSqlDialectResolver(new PostgresSqlDialect()),
                new JdbcQueryExecutor(jdbc));
    }

    @AfterEach
    void tearDown() {
        database.shutdown();
        validators.close();
    }

    private QueryRequest request(boolean direct) {
        QueryRequest request = new QueryRequest();
        if (direct) request.setTable("SALES"); else request.setDataset("sales");
        String country = direct ? "COUNTRY" : "country";
        request.setSelect(List.of(country));
        request.setSort(List.of(new SortRequest(country, SortDirection.ASC)));
        request.setFilters(List.of(new FilterRequest(direct ? "STATUS" : "status", FilterOperator.NOT_IN, List.of("CANCELLED"))));
        return request;
    }

    @Test
    void shouldApplyWhereBeforeHavingAndPageTheFilteredGroupsInBothModes() {
        for (boolean direct : List.of(false, true)) {
            QueryRequest request = request(direct);
            request.setMetrics(List.of(new MetricRequest(direct ? "AMOUNT" : "amount", MetricOperation.SUM, "total")));
            request.setGroupBy(request.getSelect());
            request.setHaving(new FilterGroupRequest(LogicalOperator.AND, List.of(
                    new FilterRequest("total", FilterOperator.GREATER_THAN, 15),
                    new FilterGroupRequest(LogicalOperator.OR, List.of(
                            new FilterRequest("total", FilterOperator.EQUALS, 30),
                            new FilterRequest("total", FilterOperator.EQUALS, 40))))));
            request.setPageSize(1);
            var first = engine.execute(request);
            assertThat(first.getRows()).hasSize(1);
            assertThat(first.getRows().getFirst().getFirst()).isEqualTo("BR");
            assertThat(first.getMetadata().isHasNext()).isTrue();
            assertThat(first.getMetadata().getRowCount()).isEqualTo(1);
            assertThat(first.getMetadata().getPageSize()).isEqualTo(1);
            request.setPage(2);
            var second = engine.execute(request);
            assertThat(second.getRows().getFirst().getFirst()).isEqualTo("DE");
            assertThat(second.getMetadata().isHasNext()).isFalse();
            request.setPage(3);
            var empty = engine.execute(request);
            assertThat(empty.getRows()).isEmpty();
            assertThat(empty.getMetadata().isHasNext()).isFalse();
        }
    }

    @Test
    void shouldApplyDistinctBeforePaginationWithoutSkippingLookAheadRows() {
        for (boolean direct : List.of(false, true)) {
            QueryRequest request = request(direct);
            request.setDistinct(true);
            request.setPageSize(2);
            assertThat(engine.preview(request).sql()).startsWith("SELECT DISTINCT").contains("LIMIT 3 OFFSET 0");
            var first = engine.execute(request);
            assertThat(first.getRows()).containsExactly(List.of("AR"), List.of("BR"));
            assertThat(first.getMetadata().isHasNext()).isTrue();
            request.setPage(2);
            var second = engine.execute(request);
            assertThat(second.getRows()).containsExactly(List.of("DE"), List.of("US"));
            assertThat(second.getMetadata().isHasNext()).isFalse();
            assertThat(new ObjectMapper().writeValueAsString(second.getMetadata())).contains("\"hasNext\":false");
        }
    }

    @Test
    void shouldHandleShortLastPageAndEmptyResults() {
        QueryRequest request = request(true);
        request.setDistinct(true);
        request.setPageSize(3);
        assertThat(engine.execute(request).getMetadata().isHasNext()).isTrue();
        request.setPage(2);
        var last = engine.execute(request);
        assertThat(last.getRows()).containsExactly(List.of("US"));
        assertThat(last.getMetadata().isHasNext()).isFalse();
        assertThat(last.getMetadata().getRowCount()).isEqualTo(1);
        request.setFilters(List.of(new FilterRequest("COUNTRY", FilterOperator.EQUALS, "missing")));
        assertThat(engine.execute(request).getRows()).isEmpty();
    }

    @Test
    void shouldSupportAggregateWithoutGroupByAndHavingNullChecks() {
        QueryRequest request = request(true);
        request.setSelect(List.of());
        request.setSort(List.of());
        request.setMetrics(List.of(new MetricRequest("AMOUNT", MetricOperation.SUM, "total")));
        request.setHaving(new FilterRequest("total", FilterOperator.IS_NOT_NULL, null));
        assertThat(engine.execute(request).getRows()).hasSize(1);
        request.setHaving(new FilterRequest("total", FilterOperator.GREATER_THAN, 1000));
        assertThat(engine.execute(request).getRows()).isEmpty();
    }

    @Test
    void shouldRepeatMetricExpressionParametersInSqlOrderForEveryDialect() {
        for (SqlDialect dialect : List.of(new PostgresSqlDialect(), new MySqlSqlDialect(), new OracleSqlDialect())) {
            AtomicReference<SqlQuery> captured = new AtomicReference<>();
            var engine = new DefaultQueryEngine(new QueryParser(), new QueryValidator(catalog, validators.getValidator()),
                    new ExecutionPlanner(catalog), new SqlTranslator(), new StaticSqlDialectResolver(dialect),
                    (request, sql) -> { captured.set(sql); return null; });
            QueryRequest request = request(true);
            request.setDistinct(true);
            var metric = new MetricRequest(null, MetricOperation.SUM, "total");
            metric.setExpression(new OperationExpression("+", List.of(new ColumnExpression("AMOUNT"), new LiteralExpression(2))));
            request.setMetrics(List.of(metric));
            request.setGroupBy(request.getSelect());
            request.setHaving(new FilterRequest("total", FilterOperator.BETWEEN, List.of(20, 80)));
            var preview = engine.preview(request);
            engine.execute(request);
            assertThat(captured.get().getParameters()).containsExactly(2, "CANCELLED", 2, 20, 80);
            assertThat(preview.sql()).isEqualTo(captured.get().getSql()).contains(" HAVING SUM(").contains("BETWEEN ? AND ?");
            assertThat(preview.sql().indexOf(" HAVING ")).isGreaterThan(preview.sql().indexOf(" GROUP BY "));
            assertThat(preview.sql().indexOf(" ORDER BY ")).isGreaterThan(preview.sql().indexOf(" HAVING "));
        }
    }

    @Test
    void shouldRejectUnsupportedHavingDistinctSortAndPaginationOverflow() {
        QueryRequest request = request(true);
        request.setHaving(new FilterRequest("unknown", FilterOperator.GREATER_THAN, 1));
        assertThatThrownBy(() -> engine.preview(request)).isInstanceOf(InvalidQueryException.class).hasMessageContaining("metric alias");
        request.setHaving(new ExistsFilterRequest(null, "SALES", null,
                br.com.lucast.atlas_query_engine.core.model.JoinType.INNER, "ID", "ID", List.of(), FilterGroupRequest.empty()));
        assertThatThrownBy(() -> engine.preview(request)).isInstanceOf(InvalidQueryException.class).hasMessageContaining("HAVING only supports");
        request.setHaving(null);
        request.setDistinct(true);
        request.setSort(List.of(new SortRequest("ID", SortDirection.ASC)));
        assertThatThrownBy(() -> engine.preview(request)).isInstanceOf(InvalidQueryException.class).hasMessageContaining("DISTINCT sort");
        request.setSort(List.of());
        request.setPage(Integer.MAX_VALUE);
        request.setPageSize(500);
        assertThatThrownBy(() -> engine.preview(request)).isInstanceOf(InvalidQueryException.class).hasMessageContaining("offset");
    }

    @Test
    void shouldRejectInvalidNotInValues() {
        QueryRequest request = request(true);
        for (Object value : List.of("scalar", List.of())) {
            request.setFilters(List.of(new FilterRequest("STATUS", FilterOperator.NOT_IN, value)));
            assertThatThrownBy(() -> engine.preview(request)).isInstanceOf(InvalidQueryException.class);
        }
    }
}
