package br.com.lucast.atlas_query_engine.core.catalog;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class DatasetRelationDefinition {

    private final String relationName;
    private final String targetDataset;
    private final String sourceColumn;
    private final String targetColumn;
    private final JoinType joinType;
}
