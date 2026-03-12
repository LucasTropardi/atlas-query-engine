package br.com.lucast.atlas_query_engine.core.model;

import tools.jackson.databind.annotation.JsonDeserialize;

@JsonDeserialize(using = ExpressionNodeDeserializer.class)
public interface ExpressionNode {
}
