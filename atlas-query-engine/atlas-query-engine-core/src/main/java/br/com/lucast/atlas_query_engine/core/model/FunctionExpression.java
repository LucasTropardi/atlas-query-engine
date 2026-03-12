package br.com.lucast.atlas_query_engine.core.model;

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
public class FunctionExpression implements ExpressionNode {

    private String function;
    private List<ExpressionNode> args = new ArrayList<>();
}
