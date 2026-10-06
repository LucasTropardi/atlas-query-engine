package br.com.lucast.atlas_query_engine.core.validator;

import br.com.lucast.atlas_query_engine.core.exception.InvalidQueryException;
import br.com.lucast.atlas_query_engine.core.model.ColumnExpression;
import br.com.lucast.atlas_query_engine.core.model.ExpressionNode;
import br.com.lucast.atlas_query_engine.core.model.FilterOperator;
import br.com.lucast.atlas_query_engine.core.model.FilterRequest;
import br.com.lucast.atlas_query_engine.core.model.FunctionExpression;
import br.com.lucast.atlas_query_engine.core.model.OperationExpression;
import br.com.lucast.atlas_query_engine.core.model.ProjectionRequest;
import br.com.lucast.atlas_query_engine.core.model.QueryRequest;
import java.lang.reflect.Array;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;


final class QueryValidationRules {
    static void validateMetricGroupingConsistency(QueryRequest request) {
        if (!request.getMetrics().isEmpty()) {
            Set<String> groupByFields = new HashSet<>(request.getGroupBy());
            for (ProjectionRequest projection : request.getProjections()) {
                validateGroupedExpression(projection.getExpression(), groupByFields);
            }
            Set<String> missing = request.getSelect().stream()
                    .filter(field -> !groupByFields.contains(field))
                    .collect(Collectors.toSet());
            if (!missing.isEmpty()) {
                throw new InvalidQueryException("groupBy must contain all selected dimension fields when metrics are present: " + missing);
            }
        }
    }

    static void validateFilterValue(FilterRequest filter) {
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

    static boolean isCollectionLike(Object value) {
        return value instanceof Collection<?> || (value != null && value.getClass().isArray());
    }

    static boolean hasTwoValues(Object value) {
        if (value instanceof Collection<?> collection) {
            return collection.size() == 2;
        }
        if (value != null && value.getClass().isArray()) {
            return Array.getLength(value) == 2;
        }
        return false;
    }

    private static void validateGroupedExpression(ExpressionNode expression, Set<String> grouped) {
        if (expression instanceof ColumnExpression column && !grouped.contains(column.getColumn())) {
            throw new InvalidQueryException("groupBy must contain projection column: " + column.getColumn());
        }
        if (expression instanceof FunctionExpression function) {
            function.getArgs().forEach(arg -> validateGroupedExpression(arg, grouped));
        }
        if (expression instanceof OperationExpression operation) {
            operation.getArgs().forEach(arg -> validateGroupedExpression(arg, grouped));
        }
    }
}
