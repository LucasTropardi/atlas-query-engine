package br.com.lucast.atlas_query_engine.core.model;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class LiteralExpression implements ExpressionNode {

    private Object literal;
}
