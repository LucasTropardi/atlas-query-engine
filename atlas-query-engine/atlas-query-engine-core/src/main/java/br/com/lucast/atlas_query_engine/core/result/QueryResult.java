package br.com.lucast.atlas_query_engine.core.result;

import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class QueryResult {

    private List<String> columns = new ArrayList<>();
    private List<List<Object>> rows = new ArrayList<>();
    private QueryMetadata metadata;
}
