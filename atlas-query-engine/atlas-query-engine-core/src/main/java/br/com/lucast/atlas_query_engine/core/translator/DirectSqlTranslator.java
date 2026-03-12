package br.com.lucast.atlas_query_engine.core.translator;

import br.com.lucast.atlas_query_engine.core.exception.InvalidQueryException;
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
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

public class DirectSqlTranslator {

    public SqlQuery translate(QueryRequest request, SqlDialect sqlDialect) {
        List<Object> parameters = new ArrayList<>();
        SqlQueryBuilder builder = new SqlQueryBuilder()
                .from(buildFromClause(request, sqlDialect))
                .pagination(sqlDialect.renderPagination(request.getPageSize(), (request.getPage() - 1) * request.getPageSize()));

        request.getSelect().stream()
                .map(field -> quoteReference(field, sqlDialect))
                .forEach(builder::addSelect);
        for (ProjectionRequest projection : request.getProjections()) {
            builder.addSelect(renderExpression(projection.getExpression(), parameters, sqlDialect)
                    + " AS " + sqlDialect.quoteIdentifier(projection.getAlias()));
        }

        for (MetricRequest metric : request.getMetrics()) {
            String metricTarget = metric.getExpression() != null
                    ? renderExpression(metric.getExpression(), parameters, sqlDialect)
                    : quoteReference(metric.getField(), sqlDialect);
            builder.addSelect(metric.getOperation().getSqlFunction()
                    + "(" + metricTarget + ") AS "
                    + sqlDialect.quoteIdentifier(metric.getAlias()));
        }

        request.getJoins().stream()
                .map(join -> buildJoinClause(join, sqlDialect))
                .forEach(builder::addJoin);

        String whereClause = buildFilterClause(request.getFilterTree(), parameters, false, sqlDialect);
        if (whereClause != null && !whereClause.isBlank()) {
            builder.addWhere(whereClause);
        }

        if (!request.getMetrics().isEmpty() && !request.getGroupBy().isEmpty()) {
            request.getGroupBy().stream()
                    .map(field -> quoteReference(field, sqlDialect))
                    .forEach(builder::addGroupBy);
        }

        Set<String> metricAliases = request.getMetrics().stream()
                .map(MetricRequest::getAlias)
                .collect(Collectors.toSet());
        for (SortRequest sort : request.getSort()) {
            String expression = metricAliases.contains(sort.getField())
                    ? sqlDialect.quoteIdentifier(sort.getField())
                    : sort.getExpression() != null
                    ? renderExpression(sort.getExpression(), parameters, sqlDialect)
                    : quoteReference(sort.getField(), sqlDialect);
            builder.addOrderBy(expression + " " + sort.getDirection().name());
        }

        return new SqlQuery(builder.build(), parameters);
    }

    private String buildFromClause(QueryRequest request, SqlDialect sqlDialect) {
        String qualifiedTable = qualifyTable(request.getSchema(), request.getTable(), sqlDialect);
        if (request.getAlias() == null || request.getAlias().isBlank()) {
            return qualifiedTable;
        }
        return qualifiedTable + " " + sqlDialect.quoteIdentifier(request.getAlias());
    }

    private String buildJoinClause(JoinRequest join, SqlDialect sqlDialect) {
        String qualifiedTable = qualifyTable(join.getSchema(), join.getTable(), sqlDialect);
        StringBuilder clause = new StringBuilder();
        clause.append(join.getType().name()).append(" JOIN ").append(qualifiedTable);
        if (join.getAlias() != null && !join.getAlias().isBlank()) {
            clause.append(" ").append(sqlDialect.quoteIdentifier(join.getAlias()));
        }
        clause.append(" ON ")
                .append(quoteReference(join.getSourceField(), sqlDialect))
                .append(" = ")
                .append(quoteReference(join.getTargetField(), sqlDialect));
        return clause.toString();
    }

    private String buildFilterClause(FilterNode filterNode, List<Object> parameters, boolean nested, SqlDialect sqlDialect) {
        if (filterNode instanceof FilterRequest filter) {
            return buildSimpleFilterClause(filter, parameters, sqlDialect);
        }
        if (filterNode instanceof ExistsFilterRequest existsFilter) {
            return buildExistsClause(existsFilter, parameters, sqlDialect);
        }
        if (filterNode instanceof FilterGroupRequest groupBinding) {
            List<String> clauses = groupBinding.getConditions().stream()
                    .map(condition -> buildFilterClause(condition, parameters, true, sqlDialect))
                    .filter(clause -> clause != null && !clause.isBlank())
                    .toList();
            if (clauses.isEmpty()) {
                return null;
            }
            String combined = String.join(" " + groupBinding.getOperator().name() + " ", clauses);
            return nested ? "(" + combined + ")" : combined;
        }
        return null;
    }

    private String buildSimpleFilterClause(FilterRequest filter, List<Object> parameters, SqlDialect sqlDialect) {
        String column = filter.getExpression() != null
                ? renderExpression(filter.getExpression(), parameters, sqlDialect)
                : quoteReference(filter.getField(), sqlDialect);
        Object value = filter.getValue();

        return switch (filter.getOperator()) {
            case EQUALS -> appendSingleValue(parameters, column + " = ?", value);
            case NOT_EQUALS -> appendSingleValue(parameters, column + " != ?", value);
            case GREATER_THAN -> appendSingleValue(parameters, column + " > ?", value);
            case GREATER_THAN_OR_EQUAL -> appendSingleValue(parameters, column + " >= ?", value);
            case LESS_THAN -> appendSingleValue(parameters, column + " < ?", value);
            case LESS_THAN_OR_EQUAL -> appendSingleValue(parameters, column + " <= ?", value);
            case LIKE -> appendSingleValue(parameters, column + " LIKE ?", value);
            case IN -> buildInClause(column, filter, value, parameters);
            case BETWEEN -> buildBetweenClause(column, filter, value, parameters);
        };
    }

    private String buildExistsClause(ExistsFilterRequest exists, List<Object> parameters, SqlDialect sqlDialect) {
        StringBuilder clause = new StringBuilder();
        clause.append("EXISTS (SELECT 1 FROM ")
                .append(qualifyTable(exists.getSchema(), exists.getTable(), sqlDialect));
        if (exists.getAlias() != null && !exists.getAlias().isBlank()) {
            clause.append(" ").append(sqlDialect.quoteIdentifier(exists.getAlias()));
        }
        for (JoinRequest join : exists.getJoins()) {
            clause.append(" ").append(buildJoinClause(join, sqlDialect));
        }
        clause.append(" WHERE ")
                .append(quoteReference(exists.getTargetField(), sqlDialect))
                .append(" = ")
                .append(quoteReference(exists.getSourceField(), sqlDialect));
        String nestedFilters = buildFilterClause(exists.getFilters(), parameters, false, sqlDialect);
        if (nestedFilters != null && !nestedFilters.isBlank()) {
            clause.append(" AND ").append(nestedFilters);
        }
        clause.append(")");
        return clause.toString();
    }

    private String appendSingleValue(List<Object> parameters, String clause, Object value) {
        parameters.add(value);
        return clause;
    }

    private String buildInClause(String column, FilterRequest filter, Object value, List<Object> parameters) {
        List<Object> values = toList(value, FilterOperator.IN, filter.getField());
        if (values.isEmpty()) {
            throw new InvalidQueryException("IN operator requires at least one value for field " + filter.getField());
        }
        parameters.addAll(values);
        String placeholders = values.stream().map(ignored -> "?").collect(Collectors.joining(", "));
        return column + " IN (" + placeholders + ")";
    }

    private String buildBetweenClause(String column, FilterRequest filter, Object value, List<Object> parameters) {
        List<Object> values = toList(value, FilterOperator.BETWEEN, filter.getField());
        if (values.size() != 2) {
            throw new InvalidQueryException("BETWEEN operator requires exactly two values for field " + filter.getField());
        }
        parameters.add(values.get(0));
        parameters.add(values.get(1));
        return column + " BETWEEN ? AND ?";
    }

    private List<Object> toList(Object value, FilterOperator operator, String fieldName) {
        if (value instanceof Collection<?> collection) {
            return new ArrayList<>(collection);
        }
        if (value != null && value.getClass().isArray()) {
            int length = Array.getLength(value);
            List<Object> values = new ArrayList<>(length);
            for (int i = 0; i < length; i++) {
                values.add(Array.get(value, i));
            }
            return values;
        }
        throw new InvalidQueryException(operator.getValue().toUpperCase() + " operator requires a collection value for field "
                + fieldName);
    }

    private String qualifyTable(String schema, String table, SqlDialect sqlDialect) {
        if (schema == null || schema.isBlank()) {
            return sqlDialect.quoteIdentifier(table);
        }
        return sqlDialect.qualifyTable(schema, table);
    }

    private String quoteReference(String reference, SqlDialect sqlDialect) {
        String[] parts = reference.split("\\.");
        if (parts.length == 0) {
            throw new InvalidQueryException("Invalid SQL reference: " + reference);
        }
        return java.util.Arrays.stream(parts)
                .map(sqlDialect::quoteIdentifier)
                .collect(Collectors.joining("."));
    }

    private String renderExpression(ExpressionNode expression, List<Object> parameters, SqlDialect sqlDialect) {
        if (expression instanceof ColumnExpression columnExpression) {
            return quoteReference(columnExpression.getColumn(), sqlDialect);
        }
        if (expression instanceof LiteralExpression literalExpression) {
            parameters.add(literalExpression.getLiteral());
            return "?";
        }
        if (expression instanceof FunctionExpression functionExpression) {
            String args = functionExpression.getArgs().stream()
                    .map(arg -> renderExpression(arg, parameters, sqlDialect))
                    .collect(Collectors.joining(", "));
            return functionExpression.getFunction().toUpperCase() + "(" + args + ")";
        }
        if (expression instanceof OperationExpression operationExpression) {
            String delimiter = " " + operationExpression.getOperator().trim() + " ";
            String combined = operationExpression.getArgs().stream()
                    .map(arg -> renderExpression(arg, parameters, sqlDialect))
                    .collect(Collectors.joining(delimiter));
            return "(" + combined + ")";
        }
        throw new InvalidQueryException("Unsupported expression type");
    }
}
