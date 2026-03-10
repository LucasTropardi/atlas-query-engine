package br.com.lucast.atlas_query_engine.core.catalog;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class SourceDefinition {

    private final String name;
    private final String dialect;
}
