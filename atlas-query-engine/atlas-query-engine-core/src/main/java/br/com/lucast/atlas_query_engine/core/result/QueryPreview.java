package br.com.lucast.atlas_query_engine.core.result;

import java.util.List;

/** Compilation details only: parameter values and connection credentials are never returned. */
public record QueryPreview(String target, String mode, String dialect, String sql,
                           List<Parameter> parameters, List<String> fields, List<String> relations) {
    public QueryPreview {
        parameters = List.copyOf(parameters);
        fields = List.copyOf(fields);
        relations = List.copyOf(relations);
    }

    /** One-based JDBC position and Java type after translation, not a database-inferred type. */
    public record Parameter(int position, String type) {}
}
