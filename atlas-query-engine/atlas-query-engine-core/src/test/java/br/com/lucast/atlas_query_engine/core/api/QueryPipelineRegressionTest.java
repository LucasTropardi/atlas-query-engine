package br.com.lucast.atlas_query_engine.core.api;

import br.com.lucast.atlas_query_engine.core.catalog.InMemoryDatasetCatalog;
import br.com.lucast.atlas_query_engine.core.exception.InvalidQueryException;
import br.com.lucast.atlas_query_engine.core.executor.JdbcQueryExecutor;
import br.com.lucast.atlas_query_engine.core.model.*;
import br.com.lucast.atlas_query_engine.core.parser.QueryParser;
import br.com.lucast.atlas_query_engine.core.planner.ExecutionPlanner;
import br.com.lucast.atlas_query_engine.core.translator.*;
import br.com.lucast.atlas_query_engine.core.validator.QueryValidator;
import jakarta.validation.Validation;
import jakarta.validation.ValidatorFactory;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.embedded.*;
import tools.jackson.databind.ObjectMapper;
import static org.assertj.core.api.Assertions.*;

class QueryPipelineRegressionTest {
    private EmbeddedDatabase database;
    private ValidatorFactory validators;
    private DefaultQueryEngine engine;

    @BeforeEach
    void setUp() {
        database = new EmbeddedDatabaseBuilder().setType(EmbeddedDatabaseType.H2).generateUniqueName(true).build();
        JdbcTemplate jdbc = new JdbcTemplate(database);
        jdbc.execute("CREATE TABLE \"customers\" (\"id\" INTEGER)");
        jdbc.execute("CREATE TABLE \"orders\" (\"customer_id\" INTEGER, \"status\" VARCHAR)");
        jdbc.execute("INSERT INTO \"customers\" VALUES (1), (2), (3)");
        jdbc.execute("INSERT INTO \"orders\" VALUES (1, 'PAID'), (2, 'PENDING')");
        validators = Validation.buildDefaultValidatorFactory();
        var catalog = new InMemoryDatasetCatalog();
        engine = new DefaultQueryEngine(new QueryParser(), new QueryValidator(catalog, validators.getValidator()),
                new ExecutionPlanner(catalog), new SqlTranslator(), new StaticSqlDialectResolver(new PostgresSqlDialect()),
                new JdbcQueryExecutor(jdbc));
    }

    @AfterEach
    void tearDown() {
        database.shutdown();
        validators.close();
    }

    @Test
    void shouldPreserveExistsAndKeepOrConditionsCorrelatedThroughEntirePipeline() {
        QueryRequest request = new ObjectMapper().readValue("""
                {
                  "table": "customers", "alias": "c", "select": ["c.id"],
                  "filters": {"exists": {
                    "table": "orders", "alias": "o",
                    "sourceField": "c.id", "targetField": "o.customer_id",
                    "filters": {"operator": "OR", "conditions": [
                      {"field": "o.status", "operator": "=", "value": "PAID"},
                      {"field": "o.status", "operator": "=", "value": "PENDING"}
                    ]}
                  }},
                  "sort": [{"field": "c.id", "direction": "asc"}]
                }
                """, QueryRequest.class);
        assertThat(engine.execute(request).getRows()).containsExactly(List.of(1), List.of(2));
    }

    @Test
    void shouldRejectAmbiguousQueryTargetBeforeExecution() {
        QueryRequest request = new QueryRequest();
        request.setTable("customers");
        request.setDataset("customers");
        request.setSelect(List.of("id"));
        assertThatThrownBy(() -> engine.execute(request)).isInstanceOf(InvalidQueryException.class)
                .hasMessageContaining("exactly one");
    }

    @Test
    void shouldRejectUnknownFilterRatherThanDiscardIt() {
        QueryRequest request = new QueryRequest();
        request.setTable("customers");
        request.setSelect(List.of("id"));
        request.setFilterTree(new FilterNode() {});
        assertThatThrownBy(() -> engine.execute(request)).isInstanceOf(InvalidQueryException.class)
                .hasMessageContaining("Unsupported filter");
    }

    @Test
    void shouldValidateFiltersInsideExists() {
        QueryRequest request = new QueryRequest();
        request.setTable("customers");
        request.setSelect(List.of("id"));
        request.setFilterTree(new ExistsFilterRequest(null, "orders", null, JoinType.INNER,
                "customers.id", "orders.customer_id", List.of(),
                new FilterRequest("status", null, "PAID")));
        assertThatThrownBy(() -> engine.execute(request)).isInstanceOf(InvalidQueryException.class)
                .hasMessageContaining("operator");
    }
}
