package br.com.lucast.atlas_query_engine.demo.controller;

import br.com.lucast.atlas_query_engine.core.api.DefaultQueryEngine;
import br.com.lucast.atlas_query_engine.core.catalog.InMemoryDatasetCatalog;
import br.com.lucast.atlas_query_engine.core.parser.QueryParser;
import br.com.lucast.atlas_query_engine.core.planner.ExecutionPlanner;
import br.com.lucast.atlas_query_engine.core.translator.*;
import br.com.lucast.atlas_query_engine.core.validator.QueryValidator;
import br.com.lucast.atlas_query_engine.demo.config.DemoDatasets;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.validation.beanvalidation.LocalValidatorFactoryBean;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;
import static org.hamcrest.Matchers.*;

class QueryDiscoveryHttpTest {
    private MockMvc mvc;
    private LocalValidatorFactoryBean validator;

    @BeforeEach
    void setUp() {
        validator = new LocalValidatorFactoryBean();
        validator.afterPropertiesSet();
        var catalog = new InMemoryDatasetCatalog(DemoDatasets.definitions());
        var engine = new DefaultQueryEngine(new QueryParser(), new QueryValidator(catalog, validator),
                new ExecutionPlanner(catalog), new SqlTranslator(), new StaticSqlDialectResolver(new PostgresSqlDialect()),
                (request, sql) -> { throw new AssertionError("HTTP preview must not execute"); });
        mvc = MockMvcBuilders.standaloneSetup(new QueryController(engine), new DatasetController(catalog))
                .setValidator(validator).setControllerAdvice(new ApiExceptionHandler()).build();
    }

    @AfterEach
    void tearDown() { validator.close(); }

    @Test
    void shouldDiscoverLogicalFieldsAndOperationsWithoutConnectionDetails() throws Exception {
        mvc.perform(get("/api/datasets")).andExpect(status().isOk())
                .andExpect(jsonPath("$[*].name", hasItem("orders")));
        mvc.perform(get("/api/datasets/orders")).andExpect(status().isOk())
                .andExpect(jsonPath("$.fields[?(@.name == 'country')].filterable", hasItem(true)))
                .andExpect(jsonPath("$.metrics[?(@.field == 'amount')].operations", hasItem(hasItem("sum"))))
                .andExpect(jsonPath("$.source").doesNotExist()).andExpect(jsonPath("$.tableName").doesNotExist());
        mvc.perform(get("/api/datasets/missing")).andExpect(status().isNotFound());
    }

    @Test
    void shouldPreviewNullChecksAndHideParameterValuesOverHttp() throws Exception {
        mvc.perform(post("/api/query/preview").contentType(MediaType.APPLICATION_JSON).content("""
                {"dataset":"orders","select":["country"],"filters":[
                  {"field":"country","operator":"is null"},
                  {"field":"status","operator":"=","value":"secret-marker"}]}
                """))
                .andExpect(status().isOk()).andExpect(jsonPath("$.sql", containsString("IS NULL")))
                .andExpect(jsonPath("$.parameters[0].position").value(1))
                .andExpect(jsonPath("$.parameters[0].type").value("String"))
                .andExpect(content().string(not(containsString("secret-marker"))));
        mvc.perform(post("/api/query/preview").contentType(MediaType.APPLICATION_JSON).content("""
                {"table":"orders","select":["id"],"filters":[{"field":"id","operator":"="}]}
                """))
                .andExpect(status().isBadRequest()).andExpect(jsonPath("$.error").value("invalid_query"));
    }

    @Test
    void shouldPreviewHavingNotInAndDistinctThroughHttp() throws Exception {
        mvc.perform(post("/api/query/preview").contentType(MediaType.APPLICATION_JSON).content("""
                {"dataset":"orders","distinct":true,"select":["country"],
                 "filters":[{"field":"status","operator":"NOT IN","value":["CANCELLED"]}],
                 "metrics":[{"field":"amount","operation":"sum","alias":"total"}],
                 "groupBy":["country"],"having":{"field":"total","operator":">","value":100},
                 "sort":[{"field":"country","direction":"asc"}],"pageSize":10}
                """))
                .andExpect(status().isOk()).andExpect(jsonPath("$.sql", startsWith("SELECT DISTINCT")))
                .andExpect(jsonPath("$.sql", containsString("NOT IN (?)")))
                .andExpect(jsonPath("$.sql", containsString("HAVING SUM(t0.amount) > ?")))
                .andExpect(jsonPath("$.sql", containsString("LIMIT 11 OFFSET 0")))
                .andExpect(jsonPath("$.parameters", hasSize(2)));
        mvc.perform(post("/api/query/preview").contentType(MediaType.APPLICATION_JSON).content("""
                {"dataset":"orders","select":["country"],"having":{"field":"total","value":100}}
                """))
                .andExpect(status().isBadRequest());
    }
}
