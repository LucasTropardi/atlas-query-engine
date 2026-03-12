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
public class JoinRequest {

    private String schema;

    @NotBlank
    private String table;

    private String alias;

    @NotNull
    private JoinType type = JoinType.LEFT;

    @NotBlank
    private String sourceField;

    @NotBlank
    private String targetField;
}
