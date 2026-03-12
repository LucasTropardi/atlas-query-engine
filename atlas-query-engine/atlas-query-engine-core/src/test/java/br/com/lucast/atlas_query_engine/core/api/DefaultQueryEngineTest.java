package br.com.lucast.atlas_query_engine.core.api;

import br.com.lucast.atlas_query_engine.core.catalog.InMemoryDatasetCatalog;
import br.com.lucast.atlas_query_engine.core.executor.QueryExecutor;
import br.com.lucast.atlas_query_engine.core.model.FilterOperator;
import br.com.lucast.atlas_query_engine.core.model.FilterRequest;
import br.com.lucast.atlas_query_engine.core.model.MetricOperation;
import br.com.lucast.atlas_query_engine.core.model.MetricRequest;
import br.com.lucast.atlas_query_engine.core.model.QueryRequest;
import br.com.lucast.atlas_query_engine.core.model.SortDirection;
import br.com.lucast.atlas_query_engine.core.model.SortRequest;
import br.com.lucast.atlas_query_engine.core.parser.QueryParser;
import br.com.lucast.atlas_query_engine.core.planner.ExecutionPlanner;
import br.com.lucast.atlas_query_engine.core.result.QueryMetadata;
import br.com.lucast.atlas_query_engine.core.result.QueryResult;
import br.com.lucast.atlas_query_engine.core.translator.PostgresSqlDialect;
import br.com.lucast.atlas_query_engine.core.translator.SqlQuery;
import br.com.lucast.atlas_query_engine.core.translator.StaticSqlDialectResolver;
import br.com.lucast.atlas_query_engine.core.translator.SqlTranslator;
import br.com.lucast.atlas_query_engine.core.validator.QueryValidator;
import jakarta.validation.Validation;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class DefaultQueryEngineTest {

    @Test
    void shouldExecuteFullPipeline() {
        CapturingExecutor executor = new CapturingExecutor();
        DefaultQueryEngine queryEngine = new DefaultQueryEngine(
                new QueryParser(),
                new QueryValidator(new InMemoryDatasetCatalog(), Validation.buildDefaultValidatorFactory().getValidator()),
                new ExecutionPlanner(new InMemoryDatasetCatalog()),
                new SqlTranslator(),
                new StaticSqlDialectResolver(new PostgresSqlDialect()),
                executor
        );

        QueryRequest request = new QueryRequest();
        request.setDataset("orders");
        request.setSelect(List.of("country", "status"));
        request.setFilters(List.of(
                new FilterRequest("status", FilterOperator.EQUALS, "PAID")
        ));
        request.setMetrics(List.of(
                new MetricRequest("id", MetricOperation.COUNT, "ordersCount")
        ));
        request.setGroupBy(List.of("country", "status"));
        request.setSort(List.of(new SortRequest("ordersCount", SortDirection.DESC)));
        request.setPage(1);
        request.setPageSize(10);

        QueryResult result = queryEngine.execute(request);

        assertThat(executor.sqlQuery).isNotNull();
        assertThat(executor.sqlQuery.getSql()).contains("COUNT(t0.id)");
        assertThat(executor.sqlQuery.getParameters()).containsExactly("PAID");
        assertThat(result.getMetadata().getDataset()).isEqualTo("orders");
    }

    private static final class CapturingExecutor implements QueryExecutor {

        private SqlQuery sqlQuery;

        @Override
        public QueryResult execute(QueryRequest request, SqlQuery sqlQuery) {
            this.sqlQuery = sqlQuery;
            return new QueryResult(
                    List.of("country", "status", "ordersCount"),
                    List.of(List.of("BR", "PAID", 2L)),
                    new QueryMetadata(request.getDataset(), 5, request.getPage(), request.getPageSize(), 1)
            );
        }
    }
}
