package br.com.lucast.atlas_query_engine.core.translator;

import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class SqlQuery {

    private final String sql;
    private final List<Object> parameters;
}
