package br.com.lucast.atlas_query_engine.core.planner;

import br.com.lucast.atlas_query_engine.core.catalog.DatasetRelationDefinition;
import br.com.lucast.atlas_query_engine.core.catalog.DatasetCatalog;
import br.com.lucast.atlas_query_engine.core.catalog.DatasetDefinition;
import br.com.lucast.atlas_query_engine.core.catalog.DimensionDefinition;
import br.com.lucast.atlas_query_engine.core.catalog.MetricDefinition;
import br.com.lucast.atlas_query_engine.core.exception.InvalidQueryException;
import br.com.lucast.atlas_query_engine.core.model.FilterGroupRequest;
import br.com.lucast.atlas_query_engine.core.model.FilterNode;
import br.com.lucast.atlas_query_engine.core.model.FilterRequest;
import br.com.lucast.atlas_query_engine.core.model.MetricRequest;
import br.com.lucast.atlas_query_engine.core.model.QueryRequest;
import br.com.lucast.atlas_query_engine.core.model.SortRequest;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ExecutionPlanner {

    private final DatasetCatalog datasetCatalog;

    public ExecutionPlanner(DatasetCatalog datasetCatalog) {
        this.datasetCatalog = datasetCatalog;
    }

    public ExecutionPlan plan(QueryRequest request) {
        DatasetDefinition dataset = datasetCatalog.findByName(request.getDataset())
                .orElseThrow();
        String baseAlias = "t0";
        Map<String, ExecutionPlan.JoinBinding> joinsByRelation = new LinkedHashMap<>();

        List<ExecutionPlan.DimensionBinding> selectedDimensions = request.getSelect().stream()
                .map(field -> bindDimension(dataset, dataset.findDimension(field).orElseThrow(), baseAlias, joinsByRelation))
                .toList();

        ExecutionPlan.FilterBindingNode filterTree =
                bindFilterNode(dataset, request.getFilterTree(), baseAlias, joinsByRelation);

        List<ExecutionPlan.MetricBinding> metrics = new ArrayList<>();
        for (MetricRequest metric : request.getMetrics()) {
            MetricDefinition metricDefinition = dataset.findMetric(metric.getField()).orElseThrow();
            String tableAlias = resolveTableAlias(dataset, metricDefinition.getRelationName(), baseAlias, joinsByRelation);
            metrics.add(new ExecutionPlan.MetricBinding(metric, metricDefinition, tableAlias));
        }

        List<ExecutionPlan.DimensionBinding> groupByDimensions = request.getGroupBy().stream()
                .map(field -> bindDimension(dataset, dataset.findDimension(field).orElseThrow(), baseAlias, joinsByRelation))
                .toList();

        Set<String> metricAliases = metrics.stream()
                .map(binding -> binding.request().getAlias())
                .collect(Collectors.toSet());

        List<ExecutionPlan.SortBinding> sortBindings = new ArrayList<>();
        for (SortRequest sort : request.getSort()) {
            if (metricAliases.contains(sort.getField())) {
                sortBindings.add(new ExecutionPlan.SortBinding(sort.getDirection(), null, sort.getField()));
            } else {
                ExecutionPlan.DimensionBinding dimensionBinding =
                        bindDimension(dataset, dataset.findDimension(sort.getField()).orElseThrow(), baseAlias, joinsByRelation);
                sortBindings.add(new ExecutionPlan.SortBinding(sort.getDirection(), dimensionBinding, null));
            }
        }

        return new ExecutionPlan(
                request,
                dataset,
                baseAlias,
                selectedDimensions,
                filterTree,
                metrics,
                groupByDimensions,
                sortBindings,
                new ArrayList<>(joinsByRelation.values()),
                request.getPageSize(),
                (request.getPage() - 1) * request.getPageSize()
        );
    }

    private ExecutionPlan.DimensionBinding bindDimension(
            DatasetDefinition dataset,
            DimensionDefinition dimension,
            String baseAlias,
            Map<String, ExecutionPlan.JoinBinding> joinsByRelation
    ) {
        String tableAlias = resolveTableAlias(dataset, dimension.getRelationName(), baseAlias, joinsByRelation);
        return new ExecutionPlan.DimensionBinding(dimension, tableAlias);
    }

    private String resolveTableAlias(
            DatasetDefinition dataset,
            String relationName,
            String baseAlias,
            Map<String, ExecutionPlan.JoinBinding> joinsByRelation
    ) {
        if (relationName == null || relationName.isBlank()) {
            return baseAlias;
        }
        ExecutionPlan.JoinBinding existing = joinsByRelation.get(relationName);
        if (existing != null) {
            return existing.targetAlias();
        }

        DatasetRelationDefinition relation = dataset.findRelation(relationName)
                .orElseThrow(() -> new InvalidQueryException("Relation is not allowed for dataset "
                        + dataset.getName() + ": " + relationName));
        DatasetDefinition targetDataset = datasetCatalog.findByName(relation.getTargetDataset())
                .orElseThrow(() -> new InvalidQueryException("Target dataset not found for relation: " + relationName));
        String targetAlias = "t" + (joinsByRelation.size() + 1);
        ExecutionPlan.JoinBinding joinBinding = new ExecutionPlan.JoinBinding(
                relationName,
                baseAlias,
                targetAlias,
                relation.getSourceColumn(),
                relation.getTargetColumn(),
                relation.getJoinType(),
                targetDataset.getQualifiedTableName()
        );
        joinsByRelation.put(relationName, joinBinding);
        return targetAlias;
    }

    private ExecutionPlan.FilterBindingNode bindFilterNode(
            DatasetDefinition dataset,
            FilterNode filterNode,
            String baseAlias,
            Map<String, ExecutionPlan.JoinBinding> joinsByRelation
    ) {
        if (filterNode instanceof FilterRequest filter) {
            ExecutionPlan.DimensionBinding dimensionBinding =
                    bindDimension(dataset, dataset.findDimension(filter.getField()).orElseThrow(), baseAlias, joinsByRelation);
            return new ExecutionPlan.FilterBinding(filter, dimensionBinding);
        }
        if (filterNode instanceof FilterGroupRequest group) {
            List<ExecutionPlan.FilterBindingNode> conditions = new ArrayList<>();
            for (FilterNode condition : group.getConditions()) {
                conditions.add(bindFilterNode(dataset, condition, baseAlias, joinsByRelation));
            }
            return new ExecutionPlan.FilterGroupBinding(group.getOperator(), conditions);
        }
        throw new InvalidQueryException("Unsupported filter node");
    }
}
