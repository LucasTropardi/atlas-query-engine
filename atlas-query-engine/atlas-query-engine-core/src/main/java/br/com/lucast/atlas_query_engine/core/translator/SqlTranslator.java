package br.com.lucast.atlas_query_engine.core.translator;

import br.com.lucast.atlas_query_engine.core.exception.InvalidQueryException;
import br.com.lucast.atlas_query_engine.core.model.FilterOperator;
import br.com.lucast.atlas_query_engine.core.planner.ExecutionPlan;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.stream.Collectors;

public class SqlTranslator {

    public SqlQuery translate(ExecutionPlan plan) {
        List<Object> parameters = new ArrayList<>();
        SqlQueryBuilder builder = new SqlQueryBuilder()
                .from(plan.getDataset().getQualifiedTableName() + " " + plan.getBaseAlias())
                .limit(plan.getLimit())
                .offset(plan.getOffset());

        buildSelectClause(plan).forEach(builder::addSelect);
        plan.getJoins().stream()
                .map(this::buildJoinClause)
                .forEach(builder::addJoin);

        String whereClause = buildFilterClause(plan.getFilterTree(), parameters, false);
        if (whereClause != null && !whereClause.isBlank()) {
            builder.addWhere(whereClause);
        }

        if (!plan.getMetrics().isEmpty() && !plan.getGroupByDimensions().isEmpty()) {
            plan.getGroupByDimensions().stream()
                    .map(ExecutionPlan.DimensionBinding::qualifiedColumn)
                    .forEach(builder::addGroupBy);
        }

        plan.getSortBindings().stream()
                .map(sort -> sort.metricSort()
                        ? quoteIdentifier(sort.metricAlias()) + " " + sort.direction().name()
                        : sort.dimensionBinding().qualifiedColumn() + " " + sort.direction().name())
                .forEach(builder::addOrderBy);

        return new SqlQuery(builder.build(), parameters);
    }

    private List<String> buildSelectClause(ExecutionPlan plan) {
        List<String> clauses = new ArrayList<>();
        plan.getSelectedDimensions().forEach(dimensionBinding ->
                clauses.add(dimensionBinding.qualifiedColumn() + " AS " + quoteIdentifier(dimensionBinding.dimension().getLogicalName())));
        plan.getMetrics().forEach(metric ->
                clauses.add(metric.request().getOperation().getSqlFunction()
                        + "(" + metric.qualifiedColumn() + ") AS "
                        + quoteIdentifier(metric.request().getAlias())));
        return clauses;
    }

    private String buildFilterClause(ExecutionPlan.FilterBindingNode filterBinding, List<Object> parameters, boolean nested) {
        if (filterBinding instanceof ExecutionPlan.FilterBinding simpleFilter) {
            return buildSimpleFilterClause(simpleFilter, parameters);
        }
        if (filterBinding instanceof ExecutionPlan.FilterGroupBinding groupBinding) {
            List<String> clauses = groupBinding.conditions().stream()
                    .map(condition -> buildFilterClause(condition, parameters, true))
                    .filter(clause -> clause != null && !clause.isBlank())
                    .toList();
            if (clauses.isEmpty()) {
                return null;
            }
            String combined = String.join(" " + groupBinding.operator().name() + " ", clauses);
            return nested ? "(" + combined + ")" : combined;
        }
        return null;
    }

    private String buildSimpleFilterClause(ExecutionPlan.FilterBinding filterBinding, List<Object> parameters) {
        String column = filterBinding.dimensionBinding().qualifiedColumn();
        Object value = filterBinding.request().getValue();
        FilterOperator operator = filterBinding.request().getOperator();
        String fieldName = filterBinding.dimensionBinding().dimension().getLogicalName();

        return switch (operator) {
            case EQUALS -> appendSingleValue(parameters, column + " = ?",
                    convertValue(filterBinding, value, fieldName));
            case NOT_EQUALS -> appendSingleValue(parameters, column + " != ?",
                    convertValue(filterBinding, value, fieldName));
            case GREATER_THAN -> appendSingleValue(parameters, column + " > ?",
                    convertValue(filterBinding, value, fieldName));
            case GREATER_THAN_OR_EQUAL -> appendSingleValue(parameters, column + " >= ?",
                    convertValue(filterBinding, value, fieldName));
            case LESS_THAN -> appendSingleValue(parameters, column + " < ?",
                    convertValue(filterBinding, value, fieldName));
            case LESS_THAN_OR_EQUAL -> appendSingleValue(parameters, column + " <= ?",
                    convertValue(filterBinding, value, fieldName));
            case LIKE -> appendSingleValue(parameters, column + " LIKE ?",
                    convertValue(filterBinding, value, fieldName));
            case IN -> buildInClause(column, filterBinding, value, parameters);
            case BETWEEN -> buildBetweenClause(column, filterBinding, value, parameters);
        };
    }

    private String appendSingleValue(List<Object> parameters, String clause, Object value) {
        parameters.add(value);
        return clause;
    }

    private String buildInClause(String column, ExecutionPlan.FilterBinding filterBinding, Object value, List<Object> parameters) {
        List<Object> values = toList(value, FilterOperator.IN, filterBinding.dimensionBinding().dimension().getLogicalName()).stream()
                .map(item -> convertValue(filterBinding, item, filterBinding.dimensionBinding().dimension().getLogicalName()))
                .toList();
        if (values.isEmpty()) {
            throw new InvalidQueryException("IN operator requires at least one value for field "
                    + filterBinding.dimensionBinding().dimension().getLogicalName());
        }
        parameters.addAll(values);
        String placeholders = values.stream().map(ignored -> "?").collect(Collectors.joining(", "));
        return column + " IN (" + placeholders + ")";
    }

    private String buildBetweenClause(String column, ExecutionPlan.FilterBinding filterBinding, Object value, List<Object> parameters) {
        List<Object> values = toList(value, FilterOperator.BETWEEN, filterBinding.dimensionBinding().dimension().getLogicalName());
        if (values.size() != 2) {
            throw new InvalidQueryException("BETWEEN operator requires exactly two values for field "
                    + filterBinding.dimensionBinding().dimension().getLogicalName());
        }
        parameters.add(convertValue(filterBinding, values.get(0), filterBinding.dimensionBinding().dimension().getLogicalName()));
        parameters.add(convertValue(filterBinding, values.get(1), filterBinding.dimensionBinding().dimension().getLogicalName()));
        return column + " BETWEEN ? AND ?";
    }

    private Object convertValue(ExecutionPlan.FilterBinding filterBinding, Object value, String fieldName) {
        return filterBinding.dimensionBinding().dimension().getFieldType().safeConvert(value, fieldName);
    }

    private String buildJoinClause(ExecutionPlan.JoinBinding joinBinding) {
        return joinBinding.joinType().name() + " JOIN " + joinBinding.targetTable() + " " + joinBinding.targetAlias()
                + " ON " + joinBinding.sourceAlias() + "." + joinBinding.sourceColumn()
                + " = " + joinBinding.targetAlias() + "." + joinBinding.targetColumn();
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

    private String quoteIdentifier(String value) {
        return "\"" + value.replace("\"", "\"\"") + "\"";
    }
}
