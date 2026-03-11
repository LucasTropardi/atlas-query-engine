package br.com.lucast.atlas_query_engine.core.translator;

import br.com.lucast.atlas_query_engine.core.catalog.InMemoryDatasetCatalog;
import br.com.lucast.atlas_query_engine.core.exception.InvalidQueryException;
import br.com.lucast.atlas_query_engine.core.model.FilterGroupRequest;
import br.com.lucast.atlas_query_engine.core.model.FilterOperator;
import br.com.lucast.atlas_query_engine.core.model.FilterRequest;
import br.com.lucast.atlas_query_engine.core.model.LogicalOperator;
import br.com.lucast.atlas_query_engine.core.model.MetricOperation;
import br.com.lucast.atlas_query_engine.core.model.MetricRequest;
import br.com.lucast.atlas_query_engine.core.model.QueryRequest;
import br.com.lucast.atlas_query_engine.core.model.SortDirection;
import br.com.lucast.atlas_query_engine.core.model.SortRequest;
import br.com.lucast.atlas_query_engine.core.parser.QueryParser;
import br.com.lucast.atlas_query_engine.core.planner.ExecutionPlanner;
import java.time.LocalDate;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

class SqlTranslatorTest {

    private final QueryParser parser = new QueryParser();
    private final ExecutionPlanner planner = new ExecutionPlanner(new InMemoryDatasetCatalog());
    private final SqlTranslator translator = new SqlTranslator();

    @Test
    void shouldTranslateAggregatedQueryToParameterizedSql() {
        QueryRequest request = new QueryRequest();
        request.setDataset("orders");
        request.setSelect(List.of("country", "status"));
        request.setFilters(List.of(
                new FilterRequest("status", FilterOperator.EQUALS, "PAID"),
                new FilterRequest("createdAt", FilterOperator.GREATER_THAN_OR_EQUAL, "2026-01-01")
        ));
        request.setMetrics(List.of(
                new MetricRequest("id", MetricOperation.COUNT, "ordersCount"),
                new MetricRequest("amount", MetricOperation.SUM, "totalAmount")
        ));
        request.setGroupBy(List.of("country", "status"));
        request.setSort(List.of(new SortRequest("totalAmount", SortDirection.DESC)));
        request.setPage(1);
        request.setPageSize(50);

        SqlQuery sqlQuery = translator.translate(planner.plan(parser.parse(request)));

        assertThat(sqlQuery.getSql()).isEqualTo(
                "SELECT t0.country AS \"country\", t0.status AS \"status\", COUNT(t0.id) AS \"ordersCount\", "
                        + "SUM(t0.amount) AS \"totalAmount\" FROM public.orders t0 WHERE t0.status = ? AND t0.created_at >= ? "
                        + "GROUP BY t0.country, t0.status ORDER BY \"totalAmount\" DESC LIMIT 50 OFFSET 0"
        );
        assertThat(sqlQuery.getParameters()).containsExactly("PAID", LocalDate.parse("2026-01-01"));
    }

    @Test
    void shouldKeepEqualsFilterParameterizedForSuspiciousValue() {
        QueryRequest request = baseRequestWithSingleFilter(new FilterRequest("status", FilterOperator.EQUALS, "abc' OR 1=1 --"));

        SqlQuery sqlQuery = translator.translate(planner.plan(parser.parse(request)));

        assertThat(sqlQuery.getSql()).contains("status = ?");
        assertThat(sqlQuery.getSql()).doesNotContain("abc' OR 1=1 --");
        assertThat(sqlQuery.getParameters()).containsExactly("abc' OR 1=1 --");
    }

    @Test
    void shouldKeepLikeFilterParameterizedForSuspiciousValue() {
        QueryRequest request = baseRequestWithSingleFilter(new FilterRequest("status", FilterOperator.LIKE, "%abc' OR 1=1 --%"));

        SqlQuery sqlQuery = translator.translate(planner.plan(parser.parse(request)));

        assertThat(sqlQuery.getSql()).contains("status LIKE ?");
        assertThat(sqlQuery.getSql()).doesNotContain("abc' OR 1=1 --");
        assertThat(sqlQuery.getParameters()).containsExactly("%abc' OR 1=1 --%");
    }

    @Test
    void shouldGenerateCorrectPlaceholdersForInOperator() {
        QueryRequest request = baseRequestWithSingleFilter(new FilterRequest("status", FilterOperator.IN, List.of("PAID", "PENDING", "CANCELLED")));

        SqlQuery sqlQuery = translator.translate(planner.plan(parser.parse(request)));

        assertThat(sqlQuery.getSql()).contains("status IN (?, ?, ?)");
        assertThat(sqlQuery.getParameters()).containsExactly("PAID", "PENDING", "CANCELLED");
    }

    @Test
    void shouldGenerateExactlyTwoPlaceholdersForBetweenOperator() {
        QueryRequest request = baseRequestWithSingleFilter(new FilterRequest("createdAt", FilterOperator.BETWEEN,
                List.of("2026-01-01", "2026-01-31")));

        SqlQuery sqlQuery = translator.translate(planner.plan(parser.parse(request)));

        assertThat(sqlQuery.getSql()).contains("created_at BETWEEN ? AND ?");
        assertThat(sqlQuery.getParameters()).containsExactly(
                LocalDate.parse("2026-01-01"),
                LocalDate.parse("2026-01-31")
        );
    }

    @Test
    void shouldFailClearlyWhenInValueIsNotACollection() {
        QueryRequest request = baseRequestWithSingleFilter(new FilterRequest("status", FilterOperator.IN, "PAID"));

        assertThatThrownBy(() -> translator.translate(planner.plan(parser.parse(request))))
                .isInstanceOf(InvalidQueryException.class)
                .hasMessageContaining("IN operator requires a collection value");
    }

    @Test
    void shouldFailClearlyWhenBetweenDoesNotReceiveTwoValues() {
        QueryRequest request = baseRequestWithSingleFilter(new FilterRequest("createdAt", FilterOperator.BETWEEN,
                List.of("2026-01-01")));

        assertThatThrownBy(() -> translator.translate(planner.plan(parser.parse(request))))
                .isInstanceOf(InvalidQueryException.class)
                .hasMessageContaining("BETWEEN operator requires exactly two values");
    }

    @Test
    void shouldTranslateSimpleOrGroupWithParentheses() {
        QueryRequest request = baseRequestWithFilterTree(new FilterGroupRequest(
                LogicalOperator.OR,
                List.of(
                        new FilterRequest("country", FilterOperator.EQUALS, "BR"),
                        new FilterRequest("country", FilterOperator.EQUALS, "US")
                )
        ));

        SqlQuery sqlQuery = translator.translate(planner.plan(parser.parse(request)));

        assertThat(sqlQuery.getSql()).contains("WHERE t0.country = ? OR t0.country = ?");
        assertThat(sqlQuery.getParameters()).containsExactly("BR", "US");
    }

    @Test
    void shouldTranslateNestedMixedLogicalGroupsPreservingPrecedence() {
        QueryRequest request = baseRequestWithFilterTree(new FilterGroupRequest(
                LogicalOperator.AND,
                List.of(
                        new FilterRequest("status", FilterOperator.EQUALS, "PAID"),
                        new FilterGroupRequest(
                                LogicalOperator.OR,
                                List.of(
                                        new FilterRequest("country", FilterOperator.EQUALS, "BR"),
                                        new FilterRequest("country", FilterOperator.EQUALS, "US")
                                )
                        )
                )
        ));

        SqlQuery sqlQuery = translator.translate(planner.plan(parser.parse(request)));

        assertThat(sqlQuery.getSql()).contains("WHERE t0.status = ? AND (t0.country = ? OR t0.country = ?)");
        assertThat(sqlQuery.getParameters()).containsExactly("PAID", "BR", "US");
    }

    @Test
    void shouldTranslateBetweenInsideLogicalGroup() {
        QueryRequest request = baseRequestWithFilterTree(new FilterGroupRequest(
                LogicalOperator.AND,
                List.of(
                        new FilterRequest("status", FilterOperator.EQUALS, "PAID"),
                        new FilterGroupRequest(
                                LogicalOperator.OR,
                                List.of(
                                        new FilterRequest("createdAt", FilterOperator.BETWEEN, List.of("2026-01-01", "2026-01-31")),
                                        new FilterRequest("country", FilterOperator.EQUALS, "BR")
                                )
                        )
                )
        ));

        SqlQuery sqlQuery = translator.translate(planner.plan(parser.parse(request)));

        assertThat(sqlQuery.getSql()).contains("WHERE t0.status = ? AND (t0.created_at BETWEEN ? AND ? OR t0.country = ?)");
        assertThat(sqlQuery.getParameters()).containsExactly(
                "PAID",
                LocalDate.parse("2026-01-01"),
                LocalDate.parse("2026-01-31"),
                "BR"
        );
    }

    @Test
    void shouldTranslateInInsideLogicalGroup() {
        QueryRequest request = baseRequestWithFilterTree(new FilterGroupRequest(
                LogicalOperator.AND,
                List.of(
                        new FilterRequest("status", FilterOperator.IN, List.of("PAID", "PENDING")),
                        new FilterRequest("country", FilterOperator.EQUALS, "BR")
                )
        ));

        SqlQuery sqlQuery = translator.translate(planner.plan(parser.parse(request)));

        assertThat(sqlQuery.getSql()).contains("WHERE t0.status IN (?, ?) AND t0.country = ?");
        assertThat(sqlQuery.getParameters()).containsExactly("PAID", "PENDING", "BR");
    }

    @Test
    void shouldKeepNestedLogicalFiltersParameterizedForSuspiciousValues() {
        QueryRequest request = baseRequestWithFilterTree(new FilterGroupRequest(
                LogicalOperator.OR,
                List.of(
                        new FilterRequest("status", FilterOperator.EQUALS, "abc' OR 1=1 --"),
                        new FilterRequest("country", FilterOperator.LIKE, "%US%' OR 1=1 --%")
                )
        ));

        SqlQuery sqlQuery = translator.translate(planner.plan(parser.parse(request)));

        assertThat(sqlQuery.getSql()).contains("status = ? OR t0.country LIKE ?");
        assertThat(sqlQuery.getSql()).doesNotContain("abc' OR 1=1 --");
        assertThat(sqlQuery.getParameters()).containsExactly("abc' OR 1=1 --", "%US%' OR 1=1 --%");
    }

    @Test
    void shouldKeepParameterOrderAcrossNestedGroups() {
        QueryRequest request = baseRequestWithFilterTree(new FilterGroupRequest(
                LogicalOperator.AND,
                List.of(
                        new FilterRequest("status", FilterOperator.EQUALS, "PAID"),
                        new FilterGroupRequest(
                                LogicalOperator.OR,
                                List.of(
                                        new FilterRequest("country", FilterOperator.IN, List.of("BR", "US")),
                                        new FilterRequest("createdAt", FilterOperator.BETWEEN, List.of("2026-01-01", "2026-01-31"))
                                )
                        ),
                        new FilterRequest("customerName", FilterOperator.LIKE, "Ana%")
                )
        ));

        SqlQuery sqlQuery = translator.translate(planner.plan(parser.parse(request)));

        assertThat(sqlQuery.getSql()).isEqualTo(
                "SELECT t0.country AS \"country\", COUNT(t0.id) AS \"orders\" FROM public.orders t0 "
                        + "LEFT JOIN public.customers t1 ON t0.customer_id = t1.id "
                        + "WHERE t0.status = ? AND (t0.country IN (?, ?) OR t0.created_at BETWEEN ? AND ?) AND t1.name LIKE ? "
                        + "GROUP BY t0.country ORDER BY \"orders\" DESC LIMIT 50 OFFSET 0"
        );
        assertThat(sqlQuery.getParameters()).containsExactly(
                "PAID",
                "BR",
                "US",
                LocalDate.parse("2026-01-01"),
                LocalDate.parse("2026-01-31"),
                "Ana%"
        );
    }

    @Test
    void shouldGenerateJoinForRelatedDatasetDimension() {
        QueryRequest request = new QueryRequest();
        request.setDataset("orders");
        request.setSelect(List.of("country", "customerName"));
        request.setMetrics(List.of(new MetricRequest("amount", MetricOperation.SUM, "revenue")));
        request.setGroupBy(List.of("country", "customerName"));
        request.setPage(1);
        request.setPageSize(50);

        SqlQuery sqlQuery = translator.translate(planner.plan(parser.parse(request)));

        assertThat(sqlQuery.getSql()).isEqualTo(
                "SELECT t0.country AS \"country\", t1.name AS \"customerName\", SUM(t0.amount) AS \"revenue\" "
                        + "FROM public.orders t0 LEFT JOIN public.customers t1 ON t0.customer_id = t1.id "
                        + "GROUP BY t0.country, t1.name LIMIT 50 OFFSET 0"
        );
    }

    private QueryRequest baseRequestWithSingleFilter(FilterRequest filter) {
        QueryRequest request = new QueryRequest();
        request.setDataset("orders");
        request.setSelect(List.of("country"));
        request.setFilters(List.of(filter));
        request.setMetrics(List.of(new MetricRequest("id", MetricOperation.COUNT, "orders")));
        request.setGroupBy(List.of("country"));
        request.setSort(List.of(new SortRequest("orders", SortDirection.DESC)));
        request.setPage(1);
        request.setPageSize(50);
        return request;
    }

    private QueryRequest baseRequestWithFilterTree(FilterGroupRequest filterTree) {
        QueryRequest request = new QueryRequest();
        request.setDataset("orders");
        request.setSelect(List.of("country"));
        request.setFilterTree(filterTree);
        request.setMetrics(List.of(new MetricRequest("id", MetricOperation.COUNT, "orders")));
        request.setGroupBy(List.of("country"));
        request.setSort(List.of(new SortRequest("orders", SortDirection.DESC)));
        request.setPage(1);
        request.setPageSize(50);
        return request;
    }
}
