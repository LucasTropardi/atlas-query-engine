package br.com.lucast.atlas_query_engine.core.validator;

import br.com.lucast.atlas_query_engine.core.catalog.DatasetCatalog;
import br.com.lucast.atlas_query_engine.core.catalog.DatasetDefinition;
import br.com.lucast.atlas_query_engine.core.catalog.DimensionDefinition;
import br.com.lucast.atlas_query_engine.core.catalog.MetricDefinition;
import br.com.lucast.atlas_query_engine.core.exception.DatasetNotFoundException;
import br.com.lucast.atlas_query_engine.core.exception.FieldNotAllowedException;
import br.com.lucast.atlas_query_engine.core.exception.InvalidQueryException;
import br.com.lucast.atlas_query_engine.core.model.FilterGroupRequest;
import br.com.lucast.atlas_query_engine.core.model.FilterNode;
import br.com.lucast.atlas_query_engine.core.model.FilterOperator;
import br.com.lucast.atlas_query_engine.core.model.FilterRequest;
import br.com.lucast.atlas_query_engine.core.model.MetricRequest;
import br.com.lucast.atlas_query_engine.core.model.QueryRequest;
import br.com.lucast.atlas_query_engine.core.model.SortRequest;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import java.lang.reflect.Array;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

public class QueryValidator {

    private final DatasetCatalog datasetCatalog;
    private final Validator validator;

    public QueryValidator(DatasetCatalog datasetCatalog, Validator validator) {
        this.datasetCatalog = datasetCatalog;
        this.validator = validator;
    }

    public DatasetDefinition validate(QueryRequest request) {
        Set<ConstraintViolation<QueryRequest>> violations = validator.validate(request);
        if (!violations.isEmpty()) {
            String message = violations.stream()
                    .map(violation -> violation.getPropertyPath() + " " + violation.getMessage())
                    .sorted()
                    .collect(Collectors.joining(", "));
            throw new InvalidQueryException(message);
        }

        if (request.getSelect().isEmpty() && request.getMetrics().isEmpty()) {
            throw new InvalidQueryException("Query must define at least one selected field or metric");
        }

        DatasetDefinition dataset = datasetCatalog.findByName(request.getDataset())
                .orElseThrow(() -> new DatasetNotFoundException(request.getDataset()));

        validateSelectedFields(dataset, request);
        validateGroupBy(dataset, request);
        validateFilters(dataset, request);
        validateMetrics(dataset, request);
        validateSort(dataset, request);
        validateMetricGroupingConsistency(request);
        return dataset;
    }

    private void validateSelectedFields(DatasetDefinition dataset, QueryRequest request) {
        for (String field : request.getSelect()) {
            DimensionDefinition dimension = dataset.findDimension(field)
                    .orElseThrow(() -> new FieldNotAllowedException("Selected field does not exist: " + field));
            if (dimension == null) {
                throw new FieldNotAllowedException("Selected field does not exist: " + field);
            }
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
        }
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

    private void validateMetricGroupingConsistency(QueryRequest request) {
        if (!request.getMetrics().isEmpty() && !request.getSelect().isEmpty()) {
            Set<String> groupByFields = new HashSet<>(request.getGroupBy());
            Set<String> missing = request.getSelect().stream()
                    .filter(field -> !groupByFields.contains(field))
                    .collect(Collectors.toSet());
            if (!missing.isEmpty()) {
                throw new InvalidQueryException("groupBy must contain all selected dimension fields when metrics are present: " + missing);
            }
        }
    }

    private void validateFilterValue(FilterRequest filter) {
        FilterOperator operator = filter.getOperator();
        Object value = filter.getValue();

        if (operator == FilterOperator.IN && !isCollectionLike(value)) {
            throw new InvalidQueryException("IN operator requires a collection value for field " + filter.getField());
        }
        if (operator == FilterOperator.BETWEEN && !hasTwoValues(value)) {
            throw new InvalidQueryException("BETWEEN operator requires exactly two values for field " + filter.getField());
        }
        if (operator == FilterOperator.LIKE && !(value instanceof String)) {
            throw new InvalidQueryException("LIKE operator requires a string value for field " + filter.getField());
        }
    }

    private boolean isCollectionLike(Object value) {
        return value instanceof Collection<?> || (value != null && value.getClass().isArray());
    }

    private boolean hasTwoValues(Object value) {
        if (value instanceof Collection<?> collection) {
            return collection.size() == 2;
        }
        if (value != null && value.getClass().isArray()) {
            return Array.getLength(value) == 2;
        }
        return false;
    }
}
