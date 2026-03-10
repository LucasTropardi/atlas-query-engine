package br.com.lucast.atlas_query_engine.demo.controller;

import br.com.lucast.atlas_query_engine.core.api.QueryEngine;
import br.com.lucast.atlas_query_engine.core.model.QueryRequest;
import br.com.lucast.atlas_query_engine.core.result.QueryResult;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/query")
public class QueryController {

    private final QueryEngine queryEngine;

    public QueryController(QueryEngine queryEngine) {
        this.queryEngine = queryEngine;
    }

    @PostMapping
    public QueryResult execute(@Valid @RequestBody QueryRequest request) {
        return queryEngine.execute(request);
    }
}
