package br.com.lucast.atlas_query_engine.core.model;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotNull;
import java.util.ArrayList;
import java.util.List;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class FilterGroupRequest implements FilterNode {

    @NotNull
    private LogicalOperator operator;

    @NotNull
    @Valid
    private List<FilterNode> conditions = new ArrayList<>();

    public static FilterGroupRequest and(List<? extends FilterNode> conditions) {
        return new FilterGroupRequest(LogicalOperator.AND, new ArrayList<>(conditions));
    }

    public static FilterGroupRequest empty() {
        return new FilterGroupRequest(LogicalOperator.AND, new ArrayList<>());
    }
}
