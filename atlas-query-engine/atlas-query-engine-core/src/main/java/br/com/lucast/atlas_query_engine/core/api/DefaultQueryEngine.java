package br.com.lucast.atlas_query_engine.core.api;

import br.com.lucast.atlas_query_engine.core.executor.QueryExecutor;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultQueryEngine implements QueryEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultQueryEngine.class);

    private final QueryParser queryParser;
    private final QueryValidator queryValidator;
    private final ExecutionPlanner executionPlanner;
    private final SqlTranslator sqlTranslator;
    private final DirectSqlTranslator directSqlTranslator;
    private final SqlDialectResolver sqlDialectResolver;
    private final QueryExecutor queryExecutor;

    public DefaultQueryEngine(
            QueryParser queryParser,
            QueryValidator queryValidator,
            ExecutionPlanner executionPlanner,
            SqlTranslator sqlTranslator,
            SqlDialectResolver sqlDialectResolver,
            QueryExecutor queryExecutor
    ) {
        this.queryParser = queryParser;
        this.queryValidator = queryValidator;
        this.executionPlanner = executionPlanner;
        this.sqlTranslator = sqlTranslator;
        this.directSqlTranslator = new DirectSqlTranslator();
        this.sqlDialectResolver = sqlDialectResolver;
        this.queryExecutor = queryExecutor;
    }

    @Override
    public QueryResult execute(QueryRequest request) {
        long startTime = System.nanoTime();
        LOGGER.info("Executing query for target={}", request.getTargetName());

        QueryRequest normalizedRequest = queryParser.parse(request);
        queryValidator.validate(normalizedRequest);
        SqlDialect sqlDialect = sqlDialectResolver.resolve(normalizedRequest);
        LOGGER.info("Resolved SQL dialect={} for target={}", sqlDialect.dialectName(), normalizedRequest.getTargetName());
        SqlQuery sqlQuery;
        if (normalizedRequest.isDirectQuery()) {
            sqlQuery = directSqlTranslator.translate(normalizedRequest, sqlDialect);
        } else {
            ExecutionPlan executionPlan = executionPlanner.plan(normalizedRequest);
            sqlQuery = sqlTranslator.translate(executionPlan, sqlDialect);
        }
        LOGGER.info("Generated SQL: {}", sqlQuery.getSql());
        LOGGER.info("SQL params: {}", sqlQuery.getParameters());

        QueryResult result = queryExecutor.execute(normalizedRequest, sqlQuery);
        long executionTimeMs = (System.nanoTime() - startTime) / 1_000_000;
        LOGGER.info("Finished query for target={} in {} ms", normalizedRequest.getTargetName(), executionTimeMs);
        return result;
    }
}
