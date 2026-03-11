package br.com.lucast.atlas_query_engine.core.model;

import org.junit.jupiter.api.Test;
import tools.jackson.databind.ObjectMapper;

import static org.assertj.core.api.Assertions.assertThat;

class QueryRequestDeserializationTest {

    private final ObjectMapper objectMapper = new ObjectMapper();

    @Test
    void shouldKeepDefaultPaginationWhenJsonOmitsPageFields() throws Exception {
        String json = """
                {
                  "dataset": "orders",
                  "select": ["country"],
                  "filters": [
                    { "field": "amount", "operator": ">", "value": 100 }
                  ],
                  "metrics": [
                    { "field": "id", "operation": "count", "alias": "orders" }
                  ],
                  "groupBy": ["country"],
                  "sort": [
                    { "field": "orders", "direction": "desc" }
                  ]
                }
                """;

        QueryRequest request = objectMapper.readValue(json, QueryRequest.class);

        assertThat(request.getPage()).isEqualTo(1);
        assertThat(request.getPageSize()).isEqualTo(50);
        assertThat(request.getFilterTree()).isInstanceOf(FilterGroupRequest.class);
    }

    @Test
    void shouldDeserializeLegacyFilterArrayAsAndGroup() throws Exception {
        String json = """
                {
                  "dataset": "orders",
                  "select": ["country"],
                  "filters": [
                    { "field": "status", "operator": "=", "value": "PAID" },
                    { "field": "country", "operator": "=", "value": "BR" }
                  ],
                  "metrics": [
                    { "field": "id", "operation": "count", "alias": "orders" }
                  ],
                  "groupBy": ["country"]
                }
                """;

        QueryRequest request = objectMapper.readValue(json, QueryRequest.class);

        assertThat(request.getFilterTree()).isInstanceOf(FilterGroupRequest.class);
        FilterGroupRequest group = (FilterGroupRequest) request.getFilterTree();
        assertThat(group.getOperator()).isEqualTo(LogicalOperator.AND);
        assertThat(group.getConditions()).hasSize(2);
        assertThat(group.getConditions()).allMatch(FilterRequest.class::isInstance);
    }

    @Test
    void shouldDeserializeStructuredLogicalFilters() throws Exception {
        String json = """
                {
                  "dataset": "orders",
                  "select": ["country"],
                  "filters": {
                    "operator": "AND",
                    "conditions": [
                      { "field": "status", "operator": "=", "value": "PAID" },
                      {
                        "operator": "OR",
                        "conditions": [
                          { "field": "country", "operator": "=", "value": "BR" },
                          { "field": "country", "operator": "=", "value": "US" }
                        ]
                      }
                    ]
                  },
                  "metrics": [
                    { "field": "id", "operation": "count", "alias": "orders" }
                  ],
                  "groupBy": ["country"]
                }
                """;

        QueryRequest request = objectMapper.readValue(json, QueryRequest.class);

        assertThat(request.getFilterTree()).isInstanceOf(FilterGroupRequest.class);
        FilterGroupRequest root = (FilterGroupRequest) request.getFilterTree();
        assertThat(root.getOperator()).isEqualTo(LogicalOperator.AND);
        assertThat(root.getConditions()).hasSize(2);
        assertThat(root.getConditions().get(0)).isInstanceOf(FilterRequest.class);
        assertThat(root.getConditions().get(1)).isInstanceOf(FilterGroupRequest.class);
        FilterGroupRequest nested = (FilterGroupRequest) root.getConditions().get(1);
        assertThat(nested.getOperator()).isEqualTo(LogicalOperator.OR);
        assertThat(nested.getConditions()).hasSize(2);
    }
}
