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
public class SortRequest {

    private String field;

    private ExpressionNode expression;

    @NotNull
    private SortDirection direction = SortDirection.ASC;

    public SortRequest(String field, SortDirection direction) {
        this.field = field;
        this.direction = direction;
    }
}
