package br.com.lucast.atlas_query_engine.core.validator;

import br.com.lucast.atlas_query_engine.core.catalog.DatasetCatalog;
import br.com.lucast.atlas_query_engine.core.catalog.DatasetDefinition;
import br.com.lucast.atlas_query_engine.core.catalog.DimensionDefinition;
import br.com.lucast.atlas_query_engine.core.catalog.MetricDefinition;
import br.com.lucast.atlas_query_engine.core.exception.InvalidQueryException;
import br.com.lucast.atlas_query_engine.core.exception.DatasetNotFoundException;
import br.com.lucast.atlas_query_engine.core.exception.FieldNotAllowedException;
import br.com.lucast.atlas_query_engine.core.model.ColumnExpression;
import br.com.lucast.atlas_query_engine.core.model.ExistsFilterRequest;
import br.com.lucast.atlas_query_engine.core.model.ExpressionNode;
import br.com.lucast.atlas_query_engine.core.model.FilterGroupRequest;
import br.com.lucast.atlas_query_engine.core.model.FilterNode;
import br.com.lucast.atlas_query_engine.core.model.FilterOperator;
import br.com.lucast.atlas_query_engine.core.model.FilterRequest;
import br.com.lucast.atlas_query_engine.core.model.FunctionExpression;
import br.com.lucast.atlas_query_engine.core.model.JoinRequest;
import br.com.lucast.atlas_query_engine.core.model.LiteralExpression;
import br.com.lucast.atlas_query_engine.core.model.MetricRequest;
import br.com.lucast.atlas_query_engine.core.model.OperationExpression;
import br.com.lucast.atlas_query_engine.core.model.ProjectionRequest;
import br.com.lucast.atlas_query_engine.core.model.QueryRequest;
import br.com.lucast.atlas_query_engine.core.model.SortRequest;
import jakarta.validation.ConstraintViolation;
import jakarta.validation.Validator;
import java.lang.reflect.Array;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

public class QueryValidator {

    private static final Pattern SIMPLE_IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final Pattern QUALIFIED_IDENTIFIER =
            Pattern.compile("[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*){0,2}");
    private static final Set<String> ALLOWED_EXPRESSION_OPERATORS = Set.of("+", "-", "*", "/");

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

        if (request.getSelect().isEmpty() && request.getProjections().isEmpty() && request.getMetrics().isEmpty()) {
            throw new InvalidQueryException("Query must define at least one selected field or metric");
        }

        if (!request.isDirectQuery() && (request.getDataset() == null || request.getDataset().isBlank())) {
            throw new InvalidQueryException("Query must define either dataset or table");
        }

        if (request.isDirectQuery()) {
            validateDirectQuery(request);
            validateMetricGroupingConsistency(request);
            return null;
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

    private void validateDirectQuery(QueryRequest request) {
        validateSimpleIdentifier(request.getSchema(), "schema");
        validateRequiredSimpleIdentifier(request.getTable(), "table");
        validateSimpleIdentifier(request.getAlias(), "alias");
        request.getSelect().forEach(field -> validateQualifiedIdentifier(field, "Selected field"));
        request.getProjections().forEach(this::validateProjection);
        request.getGroupBy().forEach(field -> validateQualifiedIdentifier(field, "Group by field"));
        validateDirectFilterNode(request.getFilterTree());
        validateDirectMetrics(request);
        validateDirectSort(request);
        for (JoinRequest join : request.getJoins()) {
            validateSimpleIdentifier(join.getSchema(), "join schema");
            validateRequiredSimpleIdentifier(join.getTable(), "join table");
            validateSimpleIdentifier(join.getAlias(), "join alias");
            validateQualifiedIdentifier(join.getSourceField(), "Join source field");
            validateQualifiedIdentifier(join.getTargetField(), "Join target field");
        }
    }

    private void validateDirectFilterNode(FilterNode filterNode) {
        if (filterNode instanceof FilterRequest filter) {
            if (filter.getExpression() != null) {
                validateExpression(filter.getExpression(), "Filter expression");
            } else {
                validateQualifiedIdentifier(filter.getField(), "Filter field");
            }
            validateFilterValue(filter);
            return;
        }
        if (filterNode instanceof ExistsFilterRequest exists) {
            validateSimpleIdentifier(exists.getSchema(), "exists schema");
            validateRequiredSimpleIdentifier(exists.getTable(), "exists table");
            validateSimpleIdentifier(exists.getAlias(), "exists alias");
            validateQualifiedIdentifier(exists.getSourceField(), "Exists source field");
            validateQualifiedIdentifier(exists.getTargetField(), "Exists target field");
            for (JoinRequest join : exists.getJoins()) {
                validateSimpleIdentifier(join.getSchema(), "exists join schema");
                validateRequiredSimpleIdentifier(join.getTable(), "exists join table");
                validateSimpleIdentifier(join.getAlias(), "exists join alias");
                validateQualifiedIdentifier(join.getSourceField(), "Exists join source field");
                validateQualifiedIdentifier(join.getTargetField(), "Exists join target field");
            }
            validateDirectFilterNode(exists.getFilters());
            return;
        }
        if (filterNode instanceof FilterGroupRequest group) {
            for (FilterNode condition : group.getConditions()) {
                validateDirectFilterNode(condition);
            }
        }
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

    private void validateDirectMetrics(QueryRequest request) {
        Set<String> aliases = new HashSet<>();
        for (MetricRequest metric : request.getMetrics()) {
            if (metric.getExpression() != null) {
                validateExpression(metric.getExpression(), "Metric expression");
            } else {
                validateQualifiedIdentifier(metric.getField(), "Metric field");
            }
            if (metric.getOperation() == null) {
                throw new InvalidQueryException("Metric operation is required for field " + metric.getField());
            }
            if (metric.getAlias() == null || metric.getAlias().isBlank()) {
                throw new InvalidQueryException("Metric alias is required for field " + metric.getField());
            }
            validateSimpleIdentifier(metric.getAlias(), "metric alias");
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

    private void validateDirectSort(QueryRequest request) {
        Set<String> metricAliases = request.getMetrics().stream()
                .map(MetricRequest::getAlias)
                .collect(Collectors.toSet());
        for (SortRequest sort : request.getSort()) {
            if (metricAliases.contains(sort.getField())) {
                validateSimpleIdentifier(sort.getField(), "Sort metric alias");
                continue;
            }
            if (sort.getExpression() != null) {
                validateExpression(sort.getExpression(), "Sort expression");
            } else {
                validateQualifiedIdentifier(sort.getField(), "Sort field");
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

    private void validateRequiredSimpleIdentifier(String value, String label) {
        if (value == null || value.isBlank()) {
            throw new InvalidQueryException(label + " is required");
        }
        validateSimpleIdentifier(value, label);
    }

    private void validateSimpleIdentifier(String value, String label) {
        if (value == null || value.isBlank()) {
            return;
        }
        if (!SIMPLE_IDENTIFIER.matcher(value).matches()) {
            throw new InvalidQueryException("Invalid " + label + ": " + value);
        }
    }

    private void validateQualifiedIdentifier(String value, String label) {
        if (value == null || value.isBlank() || !QUALIFIED_IDENTIFIER.matcher(value).matches()) {
            throw new InvalidQueryException(label + " is invalid: " + value);
        }
    }

    private void validateProjection(ProjectionRequest projection) {
        validateSimpleIdentifier(projection.getAlias(), "projection alias");
        if (projection.getExpression() == null) {
            throw new InvalidQueryException("Projection expression is required for alias " + projection.getAlias());
        }
        validateExpression(projection.getExpression(), "Projection expression");
    }

    private void validateExpression(ExpressionNode expression, String label) {
        if (expression instanceof ColumnExpression columnExpression) {
            validateQualifiedIdentifier(columnExpression.getColumn(), label + " column");
            return;
        }
        if (expression instanceof LiteralExpression) {
            return;
        }
        if (expression instanceof FunctionExpression functionExpression) {
            validateSimpleIdentifier(functionExpression.getFunction(), label + " function");
            if (functionExpression.getArgs() == null || functionExpression.getArgs().isEmpty()) {
                throw new InvalidQueryException(label + " function requires at least one argument");
            }
            functionExpression.getArgs().forEach(arg -> validateExpression(arg, label));
            return;
        }
        if (expression instanceof OperationExpression operationExpression) {
            if (operationExpression.getOperator() == null
                    || !ALLOWED_EXPRESSION_OPERATORS.contains(operationExpression.getOperator().trim())) {
                throw new InvalidQueryException(label + " operator is invalid: " + operationExpression.getOperator());
            }
            if (operationExpression.getArgs() == null || operationExpression.getArgs().size() < 2) {
                throw new InvalidQueryException(label + " operator requires at least two arguments");
            }
            operationExpression.getArgs().forEach(arg -> validateExpression(arg, label));
            return;
        }
        throw new InvalidQueryException(label + " is invalid");
    }
}
