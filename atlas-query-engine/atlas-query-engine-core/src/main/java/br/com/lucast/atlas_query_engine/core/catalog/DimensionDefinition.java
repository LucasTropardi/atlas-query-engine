package br.com.lucast.atlas_query_engine.core.catalog;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class DimensionDefinition {

    private final String logicalName;
    private final String sourceDataset;
    private final String relationName;
    private final String physicalColumn;
    private final FieldType fieldType;
    private final boolean filterable;
    private final boolean sortable;
}
