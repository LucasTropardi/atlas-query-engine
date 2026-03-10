package br.com.lucast.atlas_query_engine.core.api;

import br.com.lucast.atlas_query_engine.core.executor.QueryExecutor;
import br.com.lucast.atlas_query_engine.core.model.QueryRequest;
import br.com.lucast.atlas_query_engine.core.parser.QueryParser;
import br.com.lucast.atlas_query_engine.core.planner.ExecutionPlan;
import br.com.lucast.atlas_query_engine.core.planner.ExecutionPlanner;
import br.com.lucast.atlas_query_engine.core.result.QueryResult;
import br.com.lucast.atlas_query_engine.core.translator.SqlQuery;
import br.com.lucast.atlas_query_engine.core.translator.SqlTranslator;
import br.com.lucast.atlas_query_engine.core.validator.QueryValidator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DefaultQueryEngine implements QueryEngine {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultQueryEngine.class);

    private final QueryParser queryParser;
    private final QueryValidator queryValidator;
    private final ExecutionPlanner executionPlanner;
    private final SqlTranslator sqlTranslator;
    private final QueryExecutor queryExecutor;

    public DefaultQueryEngine(
            QueryParser queryParser,
            QueryValidator queryValidator,
            ExecutionPlanner executionPlanner,
            SqlTranslator sqlTranslator,
            QueryExecutor queryExecutor
    ) {
        this.queryParser = queryParser;
        this.queryValidator = queryValidator;
        this.executionPlanner = executionPlanner;
        this.sqlTranslator = sqlTranslator;
        this.queryExecutor = queryExecutor;
    }

    @Override
    public QueryResult execute(QueryRequest request) {
        long startTime = System.nanoTime();
        LOGGER.info("Executing query for dataset={}", request.getDataset());

        QueryRequest normalizedRequest = queryParser.parse(request);
        queryValidator.validate(normalizedRequest);
        ExecutionPlan executionPlan = executionPlanner.plan(normalizedRequest);
        SqlQuery sqlQuery = sqlTranslator.translate(executionPlan);
        LOGGER.info("Generated SQL: {}", sqlQuery.getSql());
        LOGGER.info("SQL params: {}", sqlQuery.getParameters());

        QueryResult result = queryExecutor.execute(normalizedRequest, sqlQuery);
        long executionTimeMs = (System.nanoTime() - startTime) / 1_000_000;
        LOGGER.info("Finished query for dataset={} in {} ms", normalizedRequest.getDataset(), executionTimeMs);
        return result;
    }
}
