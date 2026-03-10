package br.com.lucast.atlas_query_engine.core.planner;

import br.com.lucast.atlas_query_engine.core.catalog.InMemoryDatasetCatalog;
import br.com.lucast.atlas_query_engine.core.catalog.JoinType;
import br.com.lucast.atlas_query_engine.core.model.MetricOperation;
import br.com.lucast.atlas_query_engine.core.model.MetricRequest;
import br.com.lucast.atlas_query_engine.core.model.QueryRequest;
import br.com.lucast.atlas_query_engine.core.parser.QueryParser;
import java.util.List;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;

class ExecutionPlannerTest {

    private final QueryParser parser = new QueryParser();
    private final ExecutionPlanner planner = new ExecutionPlanner(new InMemoryDatasetCatalog());

    @Test
    void shouldRegisterJoinBindingForRelatedDimension() {
        QueryRequest request = new QueryRequest();
        request.setDataset("orders");
        request.setSelect(List.of("country", "customerName"));
        request.setMetrics(List.of(new MetricRequest("amount", MetricOperation.SUM, "revenue")));
        request.setGroupBy(List.of("country", "customerName"));

        ExecutionPlan plan = planner.plan(parser.parse(request));

        assertThat(plan.getJoins()).hasSize(1);
        ExecutionPlan.JoinBinding join = plan.getJoins().getFirst();
        assertThat(join.relationName()).isEqualTo("customer");
        assertThat(join.joinType()).isEqualTo(JoinType.LEFT);
        assertThat(join.sourceAlias()).isEqualTo("t0");
        assertThat(join.targetAlias()).isEqualTo("t1");
        assertThat(join.sourceColumn()).isEqualTo("customer_id");
        assertThat(join.targetColumn()).isEqualTo("id");
        assertThat(join.targetTable()).isEqualTo("public.customers");
    }
}
