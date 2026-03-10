package br.com.lucast.atlas_query_engine.core.executor;

import br.com.lucast.atlas_query_engine.core.model.QueryRequest;
import br.com.lucast.atlas_query_engine.core.result.QueryMetadata;
import br.com.lucast.atlas_query_engine.core.result.QueryResult;
import br.com.lucast.atlas_query_engine.core.translator.SqlQuery;
import java.sql.PreparedStatement;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;

public class JdbcQueryExecutor implements QueryExecutor {

    private static final Logger LOGGER = LoggerFactory.getLogger(JdbcQueryExecutor.class);

    private final JdbcTemplate jdbcTemplate;

    public JdbcQueryExecutor(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public QueryResult execute(QueryRequest request, SqlQuery sqlQuery) {
        long startTime = System.nanoTime();
        LOGGER.info("Executing JDBC query for dataset={}", request.getDataset());
        LOGGER.debug("Generated SQL: {}", sqlQuery.getSql());
        LOGGER.debug("SQL params: {}", sqlQuery.getParameters());
        LOGGER.debug("Rendered SQL (debug only): {}", renderSqlWithParams(sqlQuery.getSql(), sqlQuery.getParameters()));

        QueryResult result = jdbcTemplate.query(con -> {
            PreparedStatement statement = con.prepareStatement(sqlQuery.getSql());
            List<Object> parameters = sqlQuery.getParameters();
            for (int index = 0; index < parameters.size(); index++) {
                statement.setObject(index + 1, parameters.get(index));
            }
            return statement;
        }, rs -> {
            List<String> columns = new ArrayList<>();
            int columnCount = rs.getMetaData().getColumnCount();
            for (int index = 1; index <= columnCount; index++) {
                columns.add(rs.getMetaData().getColumnLabel(index));
            }

            List<List<Object>> rows = new ArrayList<>();
            while (rs.next()) {
                List<Object> row = new ArrayList<>(columnCount);
                for (int index = 1; index <= columnCount; index++) {
                    row.add(rs.getObject(index));
                }
                rows.add(row);
            }

            long executionTimeMs = (System.nanoTime() - startTime) / 1_000_000;
            QueryMetadata metadata = new QueryMetadata(
                    request.getDataset(),
                    executionTimeMs,
                    request.getPage(),
                    request.getPageSize(),
                    rows.size()
            );
            return new QueryResult(columns, rows, metadata);
        });

        QueryResult safeResult = result == null ? new QueryResult(List.of(), List.of(),
                new QueryMetadata(request.getDataset(), 0, request.getPage(), request.getPageSize(), 0)) : result;
        LOGGER.info(
                "Query returned {} row(s) in {} ms",
                safeResult.getMetadata().getRowCount(),
                safeResult.getMetadata().getExecutionTimeMs()
        );
        return safeResult;
    }

    static String renderSqlWithParams(String sql, List<Object> params) {
        String rendered = sql;
        for (Object param : params) {
            rendered = rendered.replaceFirst("\\?", formatParamForSql(param));
        }
        return rendered;
    }

    private static String formatParamForSql(Object param) {
        if (param == null) {
            return "null";
        }
        if (param instanceof Number) {
            return param.toString();
        }
        if (param instanceof LocalDate || param instanceof LocalDateTime) {
            return "'" + param + "'";
        }
        String escaped = String.valueOf(param).replace("'", "''");
        return "'" + escaped + "'";
    }
}
