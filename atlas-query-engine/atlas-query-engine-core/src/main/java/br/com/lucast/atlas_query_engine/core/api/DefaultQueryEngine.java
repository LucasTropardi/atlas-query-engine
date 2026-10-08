package br.com.lucast.atlas_query_engine.core.api;

import br.com.lucast.atlas_query_engine.core.executor.QueryExecutor;
import br.com.lucast.atlas_query_engine.core.executor.QueryExecutionResolver;
import br.com.lucast.atlas_query_engine.core.executor.ResolvedQueryExecution;
import br.com.lucast.atlas_query_engine.core.model.QueryRequest;
import br.com.lucast.atlas_query_engine.core.parser.QueryParser;
import br.com.lucast.atlas_query_engine.core.planner.ExecutionPlan;
import br.com.lucast.atlas_query_engine.core.planner.ExecutionPlanner;
import br.com.lucast.atlas_query_engine.core.result.QueryResult;
import br.com.lucast.atlas_query_engine.core.translator.SqlDialect;
import br.com.lucast.atlas_query_engine.core.translator.SqlDialectResolver;
import br.com.lucast.atlas_query_engine.core.translator.SqlQuery;
import br.com.lucast.atlas_query_engine.core.translator.SqlTranslator;
import br.com.lucast.atlas_query_engine.core.translator.DirectSqlTranslator;
import br.com.lucast.atlas_query_engine.core.validator.QueryValidator;
import br.com.lucast.atlas_query_engine.core.result.QueryPreview;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultQueryEngine implements QueryEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultQueryEngine.class);

    private final QueryParser queryParser;
    private final QueryValidator queryValidator;
    private final ExecutionPlanner executionPlanner;
    private final SqlTranslator sqlTranslator;
    private final DirectSqlTranslator directSqlTranslator;
    private final QueryExecutionResolver executionResolver;

    public DefaultQueryEngine(
            QueryParser queryParser,
            QueryValidator queryValidator,
            ExecutionPlanner executionPlanner,
            SqlTranslator sqlTranslator,
            SqlDialectResolver sqlDialectResolver,
            QueryExecutor queryExecutor
    ) {
        this(queryParser, queryValidator, executionPlanner, sqlTranslator, new DirectSqlTranslator(),
                request -> new ResolvedQueryExecution(sqlDialectResolver.resolve(request), queryExecutor));
    }

    public DefaultQueryEngine(
            QueryParser queryParser,
            QueryValidator queryValidator,
            ExecutionPlanner executionPlanner,
            SqlTranslator sqlTranslator,
            DirectSqlTranslator directSqlTranslator,
            QueryExecutionResolver executionResolver
    ) {
        this.queryParser = queryParser;
        this.queryValidator = queryValidator;
        this.executionPlanner = executionPlanner;
        this.sqlTranslator = sqlTranslator;
        this.directSqlTranslator = directSqlTranslator;
        this.executionResolver = executionResolver;
    }

    @Override
    public QueryResult execute(QueryRequest request) {
        long startTime = System.nanoTime();

        PreparedQuery prepared = prepare(request);
        QueryRequest normalizedRequest = prepared.request();
        ResolvedQueryExecution execution = prepared.execution();
        SqlQuery sqlQuery = prepared.sql();

        QueryResult result = execution.executor().execute(normalizedRequest, sqlQuery);
        long executionTimeMs = (System.nanoTime() - startTime) / 1_000_000;
        LOGGER.info("Finished query for target={} in {} ms", normalizedRequest.getTargetName(), executionTimeMs);
        return result;
    }

    @Override
    public QueryPreview preview(QueryRequest request) {
        PreparedQuery prepared = prepare(request);
        QueryRequest normalized = prepared.request();
        List<String> fields = new ArrayList<>(normalized.getSelect());
        normalized.getProjections().forEach(projection -> fields.add(projection.getAlias()));
        normalized.getMetrics().forEach(metric -> fields.add(metric.getAlias()));
        List<Object> values = prepared.sql().getParameters();
        List<QueryPreview.Parameter> parameters = IntStream.range(0, values.size())
                .mapToObj(index -> new QueryPreview.Parameter(index + 1,
                        values.get(index) == null ? "null" : values.get(index).getClass().getSimpleName())).toList();
        return new QueryPreview(normalized.getTargetName(), normalized.isDirectQuery() ? "table" : "dataset",
                prepared.execution().dialect().dialectName(), prepared.sql().getSql(), parameters, fields, prepared.relations());
    }

    private PreparedQuery prepare(QueryRequest request) {
        QueryRequest normalizedRequest = queryParser.parse(request);
        queryValidator.validate(normalizedRequest);
        ResolvedQueryExecution execution = executionResolver.resolve(normalizedRequest);
        SqlDialect sqlDialect = execution.dialect();
        LOGGER.info("Resolved SQL dialect={} for target={}", sqlDialect.dialectName(), normalizedRequest.getTargetName());
        SqlQuery sqlQuery;
        List<String> relations;
        if (normalizedRequest.isDirectQuery()) {
            sqlQuery = directSqlTranslator.translate(normalizedRequest, sqlDialect);
            relations = normalizedRequest.getJoins().stream().map(join ->
                    (join.getSchema() == null ? "" : join.getSchema() + ".") + join.getTable()).toList();
        } else {
            ExecutionPlan executionPlan = executionPlanner.plan(normalizedRequest);
            sqlQuery = sqlTranslator.translate(executionPlan, sqlDialect);
            relations = executionPlan.getJoins().stream().map(ExecutionPlan.JoinBinding::relationName).toList();
        }
        LOGGER.debug("Generated SQL: {}", sqlQuery.getSql());

        return new PreparedQuery(normalizedRequest, execution, sqlQuery, relations);
    }

    private record PreparedQuery(QueryRequest request, ResolvedQueryExecution execution,
                                 SqlQuery sql, List<String> relations) {}
}
