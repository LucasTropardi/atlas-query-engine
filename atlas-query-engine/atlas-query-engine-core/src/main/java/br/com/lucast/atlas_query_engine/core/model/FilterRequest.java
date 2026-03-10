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
public class FilterRequest {

    @NotBlank
    private String field;

    @NotNull
    private FilterOperator operator;

    @NotNull
    private Object value;
}
