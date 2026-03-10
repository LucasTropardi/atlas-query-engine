package br.com.lucast.atlas_query_engine.core.catalog;

import br.com.lucast.atlas_query_engine.core.model.MetricOperation;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class MetricDefinition {

    private final String logicalField;
    private final String sourceDataset;
    private final String relationName;
    private final String physicalColumn;
    private final FieldType fieldType;
    private final Set<MetricOperation> supportedOperations;

    public boolean supports(MetricOperation operation) {
        return supportedOperations.contains(operation);
    }
}
