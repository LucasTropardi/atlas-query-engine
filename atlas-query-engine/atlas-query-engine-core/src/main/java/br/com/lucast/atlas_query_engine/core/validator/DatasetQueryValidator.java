package br.com.lucast.atlas_query_engine.core.validator;

import br.com.lucast.atlas_query_engine.core.catalog.DatasetDefinition;
import br.com.lucast.atlas_query_engine.core.catalog.DimensionDefinition;
import br.com.lucast.atlas_query_engine.core.catalog.MetricDefinition;
import br.com.lucast.atlas_query_engine.core.exception.InvalidQueryException;
import br.com.lucast.atlas_query_engine.core.exception.FieldNotAllowedException;
import br.com.lucast.atlas_query_engine.core.model.FilterGroupRequest;
import br.com.lucast.atlas_query_engine.core.model.FilterNode;
import br.com.lucast.atlas_query_engine.core.model.FilterRequest;
import br.com.lucast.atlas_query_engine.core.model.MetricRequest;
import br.com.lucast.atlas_query_engine.core.model.QueryRequest;
import br.com.lucast.atlas_query_engine.core.model.SortRequest;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static br.com.lucast.atlas_query_engine.core.validator.QueryValidationRules.validateFilterValue;
import static br.com.lucast.atlas_query_engine.core.validator.ExpressionValidator.validateRequiredSimpleIdentifier;

final class DatasetQueryValidator {
    void validate(DatasetDefinition dataset, QueryRequest request) {
        if (!request.getProjections().isEmpty() || !request.getJoins().isEmpty()
                || request.getSchema() != null || request.getAlias() != null
                || request.getMetrics().stream().anyMatch(metric -> metric.getExpression() != null)
                || request.getSort().stream().anyMatch(sort -> sort.getExpression() != null)) {
            throw new InvalidQueryException("Dataset queries do not support direct projections, joins, schema, alias or expressions");
        }
        validateSelectedFields(dataset, request);
        validateGroupBy(dataset, request);
        validateFilters(dataset, request);
        validateMetrics(dataset, request);
        validateSort(dataset, request);
    }

    private void validateSelectedFields(DatasetDefinition dataset, QueryRequest request) {
        for (String field : request.getSelect()) {
            dataset.findDimension(field)
                    .orElseThrow(() -> new FieldNotAllowedException("Selected field does not exist: " + field));
        }
    }

    private void validateGroupBy(DatasetDefinition dataset, QueryRequest request) {
        for (String field : request.getGroupBy()) {
            dataset.findDimension(field)
                    .orElseThrow(() -> new FieldNotAllowedException("Group by field does not exist: " + field));
        }
    }

    private void validateFilters(DatasetDefinition dataset, QueryRequest request) {
        validateFilterNode(dataset, request.getFilterTree());
    }

    private void validateFilterNode(DatasetDefinition dataset, FilterNode filterNode) {
        if (filterNode instanceof FilterRequest filter) {
            if (filter.getExpression() != null) {
                throw new InvalidQueryException("Dataset queries do not support filter expressions");
            }
            DimensionDefinition dimension = dataset.findDimension(filter.getField())
                    .orElseThrow(() -> new FieldNotAllowedException("Filter field does not exist: " + filter.getField()));
            if (!dimension.isFilterable()) {
                throw new FieldNotAllowedException("Field is not filterable: " + filter.getField());
            }
            validateFilterValue(filter);
            return;
        }
        if (filterNode instanceof FilterGroupRequest group) {
            for (FilterNode condition : group.getConditions()) {
                validateFilterNode(dataset, condition);
            }
            return;
        }
        throw new InvalidQueryException("Unsupported filter node for dataset query");
    }

    private void validateMetrics(DatasetDefinition dataset, QueryRequest request) {
        Set<String> aliases = new HashSet<>();
        for (MetricRequest metric : request.getMetrics()) {
            MetricDefinition metricDefinition = dataset.findMetric(metric.getField())
                    .orElseThrow(() -> new FieldNotAllowedException("Metric field does not exist: " + metric.getField()));
            if (!metricDefinition.supports(metric.getOperation())) {
                throw new FieldNotAllowedException(
                        "Metric operation " + metric.getOperation().getValue() + " is not allowed for field " + metric.getField());
            }
            validateRequiredSimpleIdentifier(metric.getAlias(), "metric alias");
            if (!aliases.add(metric.getAlias())) {
                throw new InvalidQueryException("Metric aliases must be unique: " + metric.getAlias());
            }
        }
    }

    private void validateSort(DatasetDefinition dataset, QueryRequest request) {
        Set<String> metricAliases = request.getMetrics().stream()
                .map(MetricRequest::getAlias)
                .collect(Collectors.toSet());
        for (SortRequest sort : request.getSort()) {
            if (metricAliases.contains(sort.getField())) {
                continue;
            }
            DimensionDefinition dimension = dataset.findDimension(sort.getField())
                    .orElseThrow(() -> new FieldNotAllowedException("Sort field does not exist: " + sort.getField()));
            if (!dimension.isSortable()) {
                throw new FieldNotAllowedException("Field is not sortable: " + sort.getField());
            }
        }
    }
}
