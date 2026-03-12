package br.com.lucast.atlas_query_engine.core.parser;

import br.com.lucast.atlas_query_engine.core.model.FilterGroupRequest;
import br.com.lucast.atlas_query_engine.core.model.FilterNode;
import br.com.lucast.atlas_query_engine.core.model.FilterRequest;
import br.com.lucast.atlas_query_engine.core.model.JoinRequest;
import br.com.lucast.atlas_query_engine.core.model.MetricRequest;
import br.com.lucast.atlas_query_engine.core.model.ProjectionRequest;
import br.com.lucast.atlas_query_engine.core.model.QueryRequest;
import br.com.lucast.atlas_query_engine.core.model.SortDirection;
import br.com.lucast.atlas_query_engine.core.model.SortRequest;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

public class QueryParser {

    public QueryRequest parse(QueryRequest request) {
        QueryRequest normalized = new QueryRequest();
        normalized.setDataset(trimToNull(request.getDataset()));
        normalized.setSchema(trimToNull(request.getSchema()));
        normalized.setTable(trimToNull(request.getTable()));
        normalized.setAlias(trimToNull(request.getAlias()));
        normalized.setConnection(trimToNull(request.getConnection()));
        normalized.setSelect(normalizeStrings(request.getSelect()));
        normalized.setProjections(normalizeProjections(request.getProjections()));
        normalized.setFilterTree(normalizeFilterNode(request.getFilterTree()));
        normalized.setMetrics(normalizeMetrics(request.getMetrics()));
        normalized.setGroupBy(normalizeStrings(request.getGroupBy()));
        normalized.setSort(normalizeSort(request.getSort()));
        normalized.setJoins(normalizeJoins(request.getJoins()));
        normalized.setPage(request.getPage() <= 0 ? 1 : request.getPage());
        normalized.setPageSize(request.getPageSize() <= 0 ? 50 : request.getPageSize());
        return normalized;
    }

    private List<String> normalizeStrings(List<String> values) {
        List<String> normalized = new ArrayList<>();
        if (values == null) {
            return normalized;
        }
        for (String value : values) {
            normalized.add(trimToNull(value));
        }
        return normalized;
    }

    private FilterNode normalizeFilterNode(FilterNode filterNode) {
        if (filterNode == null) {
            return FilterGroupRequest.empty();
        }
        if (filterNode instanceof FilterRequest filter) {
            FilterRequest normalized = new FilterRequest(
                    trimToNull(filter.getField()),
                    filter.getOperator(),
                    filter.getValue()
            );
            normalized.setExpression(filter.getExpression());
            return normalized;
        }
        if (filterNode instanceof FilterGroupRequest group) {
            List<FilterNode> normalizedConditions = new ArrayList<>();
            for (FilterNode condition : group.getConditions()) {
                normalizedConditions.add(normalizeFilterNode(condition));
            }
            return new FilterGroupRequest(group.getOperator(), normalizedConditions);
        }
        return FilterGroupRequest.empty();
    }

    private List<MetricRequest> normalizeMetrics(List<MetricRequest> metrics) {
        List<MetricRequest> normalized = new ArrayList<>();
        if (metrics == null) {
            return normalized;
        }
        for (MetricRequest metric : metrics) {
            String field = trimToNull(metric.getField());
            String alias = trimToNull(metric.getAlias());
            if (alias == null && metric.getOperation() != null && field != null) {
                alias = metric.getOperation().getValue() + "_" + field;
            }
            MetricRequest normalizedMetric = new MetricRequest(field, metric.getOperation(), alias);
            normalizedMetric.setExpression(metric.getExpression());
            normalized.add(normalizedMetric);
        }
        return normalized;
    }

    private List<SortRequest> normalizeSort(List<SortRequest> sortItems) {
        List<SortRequest> normalized = new ArrayList<>();
        if (sortItems == null) {
            return normalized;
        }
        for (SortRequest sortItem : sortItems) {
            SortDirection direction = Objects.requireNonNullElse(sortItem.getDirection(), SortDirection.ASC);
            SortRequest sortRequest = new SortRequest(trimToNull(sortItem.getField()), direction);
            sortRequest.setExpression(sortItem.getExpression());
            normalized.add(sortRequest);
        }
        return normalized;
    }

    private List<ProjectionRequest> normalizeProjections(List<ProjectionRequest> projections) {
        List<ProjectionRequest> normalized = new ArrayList<>();
        if (projections == null) {
            return normalized;
        }
        for (ProjectionRequest projection : projections) {
            normalized.add(new ProjectionRequest(trimToNull(projection.getAlias()), projection.getExpression()));
        }
        return normalized;
    }

    private List<JoinRequest> normalizeJoins(List<JoinRequest> joins) {
        List<JoinRequest> normalized = new ArrayList<>();
        if (joins == null) {
            return normalized;
        }
        for (JoinRequest join : joins) {
            normalized.add(new JoinRequest(
                    trimToNull(join.getSchema()),
                    trimToNull(join.getTable()),
                    trimToNull(join.getAlias()),
                    join.getType(),
                    trimToNull(join.getSourceField()),
                    trimToNull(join.getTargetField())
            ));
        }
        return normalized;
    }

    private String trimToNull(String value) {
        if (value == null) {
            return null;
        }
        String trimmed = value.trim();
        return trimmed.isEmpty() ? null : trimmed;
    }
}
