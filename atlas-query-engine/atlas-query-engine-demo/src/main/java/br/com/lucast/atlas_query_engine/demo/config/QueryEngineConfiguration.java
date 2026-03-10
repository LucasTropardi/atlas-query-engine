package br.com.lucast.atlas_query_engine.demo.config;

import br.com.lucast.atlas_query_engine.core.api.DefaultQueryEngine;
import br.com.lucast.atlas_query_engine.core.api.QueryEngine;
import br.com.lucast.atlas_query_engine.core.catalog.DatasetCatalog;
import br.com.lucast.atlas_query_engine.core.catalog.InMemoryDatasetCatalog;
import br.com.lucast.atlas_query_engine.core.executor.JdbcQueryExecutor;
import br.com.lucast.atlas_query_engine.core.executor.QueryExecutor;
import br.com.lucast.atlas_query_engine.core.parser.QueryParser;
import br.com.lucast.atlas_query_engine.core.planner.ExecutionPlanner;
import br.com.lucast.atlas_query_engine.core.translator.SqlTranslator;
import br.com.lucast.atlas_query_engine.core.validator.QueryValidator;
import jakarta.validation.Validator;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

@Configuration
public class QueryEngineConfiguration {

    @Bean
    DatasetCatalog datasetCatalog() {
        return new InMemoryDatasetCatalog();
    }

    @Bean
    QueryParser queryParser() {
        return new QueryParser();
    }

    @Bean
    QueryValidator queryValidator(DatasetCatalog datasetCatalog, Validator validator) {
        return new QueryValidator(datasetCatalog, validator);
    }

    @Bean
    ExecutionPlanner executionPlanner(DatasetCatalog datasetCatalog) {
        return new ExecutionPlanner(datasetCatalog);
    }

    @Bean
    SqlTranslator sqlTranslator() {
        return new SqlTranslator();
    }

    @Bean
    QueryExecutor queryExecutor(JdbcTemplate jdbcTemplate) {
        return new JdbcQueryExecutor(jdbcTemplate);
    }

    @Bean
    QueryEngine queryEngine(
            QueryParser queryParser,
            QueryValidator queryValidator,
            ExecutionPlanner executionPlanner,
            SqlTranslator sqlTranslator,
            QueryExecutor queryExecutor
    ) {
        return new DefaultQueryEngine(queryParser, queryValidator, executionPlanner, sqlTranslator, queryExecutor);
    }
}
