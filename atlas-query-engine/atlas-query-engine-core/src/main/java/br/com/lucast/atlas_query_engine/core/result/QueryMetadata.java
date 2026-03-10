package br.com.lucast.atlas_query_engine.core.result;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class QueryMetadata {

    private String dataset;
    private long executionTimeMs;
    private int page;
    private int pageSize;
    private int rowCount;
}
