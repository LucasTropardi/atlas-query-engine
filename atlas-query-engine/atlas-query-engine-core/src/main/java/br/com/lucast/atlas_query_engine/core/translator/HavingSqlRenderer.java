package br.com.lucast.atlas_query_engine.core.translator;

import br.com.lucast.atlas_query_engine.core.exception.InvalidQueryException;
import br.com.lucast.atlas_query_engine.core.model.FilterGroupRequest;
import br.com.lucast.atlas_query_engine.core.model.FilterNode;
import br.com.lucast.atlas_query_engine.core.model.FilterOperator;
import br.com.lucast.atlas_query_engine.core.model.FilterRequest;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;

/** Resolves aggregate expressions at each occurrence so literal parameter order follows SQL order. */
final class HavingSqlRenderer {
    private HavingSqlRenderer() {}

    static String render(FilterNode node, List<Object> parameters, Function<String, String> metric) {
        if (node instanceof FilterGroupRequest group) {
            List<String> clauses = group.getConditions().stream()
                    .map(child -> render(child, parameters, metric)).filter(clause -> !clause.isBlank()).toList();
            return clauses.isEmpty() ? "" : "(" + String.join(" " + group.getOperator().name() + " ", clauses) + ")";
        }
        if (!(node instanceof FilterRequest filter)) {
            throw new InvalidQueryException("HAVING only supports filters on metric aliases and logical groups");
        }
        String expression = metric.apply(filter.getField());
        if (filter.getOperator() == FilterOperator.IS_NULL) return expression + " IS NULL";
        if (filter.getOperator() == FilterOperator.IS_NOT_NULL) return expression + " IS NOT NULL";
        if (filter.getOperator() == FilterOperator.IN || filter.getOperator() == FilterOperator.NOT_IN
                || filter.getOperator() == FilterOperator.BETWEEN) {
            List<Object> values = values(filter.getValue());
            if (filter.getOperator() == FilterOperator.BETWEEN) {
                if (values.size() != 2) throw new InvalidQueryException("BETWEEN requires exactly two values");
                parameters.addAll(values);
                return expression + " BETWEEN ? AND ?";
            }
            if (values.isEmpty()) throw new InvalidQueryException("IN/NOT IN requires at least one value");
            parameters.addAll(values);
            return expression + (filter.getOperator() == FilterOperator.IN ? " IN (" : " NOT IN (")
                    + values.stream().map(value -> "?").collect(Collectors.joining(", ")) + ")";
        }
        parameters.add(filter.getValue());
        return expression + " " + filter.getOperator().getValue().toUpperCase(java.util.Locale.ROOT) + " ?";
    }

    private static List<Object> values(Object value) {
        if (value instanceof Collection<?> collection) return new ArrayList<>(collection);
        if (value != null && value.getClass().isArray()) {
            List<Object> values = new ArrayList<>();
            for (int index = 0; index < Array.getLength(value); index++) values.add(Array.get(value, index));
            return values;
        }
        throw new InvalidQueryException("HAVING collection operator requires a collection value");
    }
}
