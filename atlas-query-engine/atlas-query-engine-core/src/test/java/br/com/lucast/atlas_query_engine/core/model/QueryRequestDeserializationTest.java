package br.com.lucast.atlas_query_engine.core.model;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

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
    }
}
