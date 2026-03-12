package br.com.lucast.atlas_query_engine.core.translator;

import java.util.ArrayList;
import java.util.List;
import java.util.StringJoiner;

class SqlQueryBuilder {

    private final List<String> selectColumns = new ArrayList<>();
    private String fromClause;
    private final List<String> joinClauses = new ArrayList<>();
    private final List<String> whereConditions = new ArrayList<>();
    private final List<String> groupByColumns = new ArrayList<>();
    private final List<String> orderByColumns = new ArrayList<>();
    private String paginationClause;

    SqlQueryBuilder addSelect(String expression) {
        selectColumns.add(expression);
        return this;
    }

    SqlQueryBuilder from(String table) {
        this.fromClause = table;
        return this;
    }

    SqlQueryBuilder addWhere(String condition) {
        whereConditions.add(condition);
        return this;
    }

    SqlQueryBuilder addJoin(String joinClause) {
        joinClauses.add(joinClause);
        return this;
    }

    SqlQueryBuilder addGroupBy(String column) {
        groupByColumns.add(column);
        return this;
    }

    SqlQueryBuilder addOrderBy(String expression) {
        orderByColumns.add(expression);
        return this;
    }

    SqlQueryBuilder pagination(String clause) {
        this.paginationClause = clause;
        return this;
    }

    String build() {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT ").append(String.join(", ", selectColumns));
        sql.append(" FROM ").append(fromClause);
        if (!joinClauses.isEmpty()) {
            sql.append(" ").append(String.join(" ", joinClauses));
        }

        if (!whereConditions.isEmpty()) {
            sql.append(" WHERE ").append(String.join(" AND ", whereConditions));
        }
        if (!groupByColumns.isEmpty()) {
            sql.append(" GROUP BY ").append(String.join(", ", groupByColumns));
        }
        if (!orderByColumns.isEmpty()) {
            sql.append(" ORDER BY ").append(String.join(", ", orderByColumns));
        }
        if (paginationClause != null && !paginationClause.isBlank()) {
            sql.append(" ").append(paginationClause);
        }
        return sql.toString();
    }
}
