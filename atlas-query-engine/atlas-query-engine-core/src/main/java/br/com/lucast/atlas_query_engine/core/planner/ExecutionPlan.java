package br.com.lucast.atlas_query_engine.core.planner;

import br.com.lucast.atlas_query_engine.core.catalog.DatasetDefinition;
import br.com.lucast.atlas_query_engine.core.catalog.DimensionDefinition;
import br.com.lucast.atlas_query_engine.core.catalog.JoinType;
import br.com.lucast.atlas_query_engine.core.catalog.MetricDefinition;
import br.com.lucast.atlas_query_engine.core.model.LogicalOperator;
import br.com.lucast.atlas_query_engine.core.model.FilterRequest;
import br.com.lucast.atlas_query_engine.core.model.MetricRequest;
import br.com.lucast.atlas_query_engine.core.model.QueryRequest;
import br.com.lucast.atlas_query_engine.core.model.SortDirection;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class ExecutionPlan {

    private final QueryRequest request;
    private final DatasetDefinition dataset;
    private final String baseAlias;
    private final List<DimensionBinding> selectedDimensions;
    private final FilterBindingNode filterTree;
    private final List<MetricBinding> metrics;
    private final List<DimensionBinding> groupByDimensions;
    private final List<SortBinding> sortBindings;
    private final List<JoinBinding> joins;
    private final int limit;
    private final int offset;

    public record DimensionBinding(DimensionDefinition dimension, String tableAlias) {

        public String qualifiedColumn() {
            return tableAlias + "." + dimension.getPhysicalColumn();
        }
    }

    public sealed interface FilterBindingNode permits FilterBinding, FilterGroupBinding {
    }

    public record FilterBinding(FilterRequest request, DimensionBinding dimensionBinding) implements FilterBindingNode {
    }

    public record FilterGroupBinding(LogicalOperator operator, List<FilterBindingNode> conditions) implements FilterBindingNode {
    }

    public record MetricBinding(MetricRequest request, MetricDefinition metricDefinition, String tableAlias) {

        public String qualifiedColumn() {
            return tableAlias + "." + metricDefinition.getPhysicalColumn();
        }
    }

    public record SortBinding(SortDirection direction, DimensionBinding dimensionBinding, String metricAlias) {

        public boolean metricSort() {
            return metricAlias != null;
        }
    }

    public record JoinBinding(
            String relationName,
            String sourceAlias,
            String targetAlias,
            String sourceColumn,
            String targetColumn,
            JoinType joinType,
            String targetTable
    ) {
    }
}
