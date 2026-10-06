package br.com.lucast.atlas_query_engine.core.validator;

import br.com.lucast.atlas_query_engine.core.exception.InvalidQueryException;
import br.com.lucast.atlas_query_engine.core.model.ExistsFilterRequest;
import br.com.lucast.atlas_query_engine.core.model.FilterGroupRequest;
import br.com.lucast.atlas_query_engine.core.model.FilterNode;
import br.com.lucast.atlas_query_engine.core.model.FilterRequest;
import br.com.lucast.atlas_query_engine.core.model.JoinRequest;
import br.com.lucast.atlas_query_engine.core.model.MetricRequest;
import br.com.lucast.atlas_query_engine.core.model.ProjectionRequest;
import br.com.lucast.atlas_query_engine.core.model.QueryRequest;
import br.com.lucast.atlas_query_engine.core.model.SortRequest;
import java.util.HashSet;
import java.util.Set;
import java.util.stream.Collectors;

import static br.com.lucast.atlas_query_engine.core.validator.QueryValidationRules.validateFilterValue;
import static br.com.lucast.atlas_query_engine.core.validator.ExpressionValidator.validateRequiredSimpleIdentifier;
import static br.com.lucast.atlas_query_engine.core.validator.ExpressionValidator.validateSimpleIdentifier;
import static br.com.lucast.atlas_query_engine.core.validator.ExpressionValidator.validateQualifiedIdentifier;
import static br.com.lucast.atlas_query_engine.core.validator.ExpressionValidator.validateExpression;

final class DirectQueryValidator {
    void validate(QueryRequest request) {
        validateSimpleIdentifier(request.getSchema(), "schema");
        validateRequiredSimpleIdentifier(request.getTable(), "table");
        validateSimpleIdentifier(request.getAlias(), "alias");
        request.getSelect().forEach(field -> validateQualifiedIdentifier(field, "Selected field"));
        request.getProjections().forEach(this::validateProjection);
        Set<String> outputAliases = new HashSet<>(request.getSelect());
        for (ProjectionRequest projection : request.getProjections()) {
            if (!outputAliases.add(projection.getAlias())) {
                throw new InvalidQueryException("Output aliases must be unique: " + projection.getAlias());
            }
        }
        for (MetricRequest metric : request.getMetrics()) {
            if (!outputAliases.add(metric.getAlias())) {
                throw new InvalidQueryException("Output aliases must be unique: " + metric.getAlias());
            }
        }
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
            return;
        }
        throw new InvalidQueryException("Unsupported filter node");
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

    private void validateProjection(ProjectionRequest projection) {
        validateSimpleIdentifier(projection.getAlias(), "projection alias");
        if (projection.getExpression() == null) {
            throw new InvalidQueryException("Projection expression is required for alias " + projection.getAlias());
        }
        validateExpression(projection.getExpression(), "Projection expression");
    }
}
