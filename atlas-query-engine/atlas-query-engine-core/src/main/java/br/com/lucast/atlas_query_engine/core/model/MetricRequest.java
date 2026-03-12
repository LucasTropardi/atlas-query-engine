package br.com.lucast.atlas_query_engine.core.model;

import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class MetricRequest {

    private String field;

    private ExpressionNode expression;

    @NotNull
    private MetricOperation operation;

    private String alias;

    public MetricRequest(String field, MetricOperation operation, String alias) {
        this.field = field;
        this.operation = operation;
        this.alias = alias;
    }
}
