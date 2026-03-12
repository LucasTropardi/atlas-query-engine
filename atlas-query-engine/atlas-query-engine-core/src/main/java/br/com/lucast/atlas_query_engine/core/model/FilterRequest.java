package br.com.lucast.atlas_query_engine.core.model;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class FilterRequest implements FilterNode {

    private String field;

    private ExpressionNode expression;

    @NotNull
    private FilterOperator operator;

    @NotNull
    private Object value;

    public FilterRequest(String field, FilterOperator operator, Object value) {
        this.field = field;
        this.operator = operator;
        this.value = value;
    }
}
