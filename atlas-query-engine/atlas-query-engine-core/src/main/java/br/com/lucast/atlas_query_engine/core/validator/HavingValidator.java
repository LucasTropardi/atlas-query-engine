package br.com.lucast.atlas_query_engine.core.validator;

import br.com.lucast.atlas_query_engine.core.exception.InvalidQueryException;
import br.com.lucast.atlas_query_engine.core.model.FilterGroupRequest;
import br.com.lucast.atlas_query_engine.core.model.FilterNode;
import br.com.lucast.atlas_query_engine.core.model.FilterRequest;
import br.com.lucast.atlas_query_engine.core.model.MetricRequest;
import br.com.lucast.atlas_query_engine.core.model.QueryRequest;
import java.util.Set;
import java.util.stream.Collectors;

final class HavingValidator {
    static void validate(QueryRequest request) {
        Set<String> aliases = request.getMetrics().stream().map(MetricRequest::getAlias).collect(Collectors.toSet());
        validateNode(request.getHaving(), aliases);
    }

    private static void validateNode(FilterNode node, Set<String> aliases) {
        if (node instanceof FilterGroupRequest group) {
            group.getConditions().forEach(child -> validateNode(child, aliases));
        } else if (node instanceof FilterRequest filter) {
            if (filter.getExpression() != null || !aliases.contains(filter.getField())) {
                throw new InvalidQueryException("HAVING field must reference a declared metric alias: " + filter.getField());
            }
            QueryValidationRules.validateFilterValue(filter);
        } else {
            throw new InvalidQueryException("HAVING only supports filters on metric aliases and logical groups");
        }
    }
}
