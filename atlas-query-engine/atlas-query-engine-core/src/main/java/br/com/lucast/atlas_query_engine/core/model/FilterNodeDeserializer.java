package br.com.lucast.atlas_query_engine.core.model;

import tools.jackson.core.JsonParser;
import tools.jackson.core.JacksonException;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ValueDeserializer;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class FilterNodeDeserializer extends ValueDeserializer<FilterNode> {

    @Override
    public FilterNode deserialize(JsonParser parser, DeserializationContext context) throws JacksonException {
        JsonNode node = parser.readValueAsTree();
        return deserializeNode(parser, node);
    }

    private FilterNode deserializeNode(JsonParser parser, JsonNode node) throws JacksonException {
        if (node == null || node.isNull()) {
            return FilterGroupRequest.empty();
        }
        if (node.isArray()) {
            List<FilterNode> conditions = new ArrayList<>();
            for (JsonNode child : node) {
                conditions.add(deserializeNode(parser, child));
            }
            return FilterGroupRequest.and(conditions);
        }
        if (node.isObject() && node.has("conditions")) {
            JsonNode operatorNode = node.get("operator");
            JsonNode conditionsNode = node.get("conditions");
            List<FilterNode> conditions = new ArrayList<>();
            if (conditionsNode != null && conditionsNode.isArray()) {
                for (JsonNode child : conditionsNode) {
                    conditions.add(deserializeNode(parser, child));
                }
            }
            return new FilterGroupRequest(
                    operatorNode == null || operatorNode.isNull() ? null : LogicalOperator.fromValue(operatorNode.asText()),
                    conditions
            );
        }
        if (node.isObject() && node.has("exists")) {
            JsonNode existsNode = node.get("exists");
            List<JoinRequest> joins = new ArrayList<>();
            JsonNode joinsNode = existsNode.get("joins");
            if (joinsNode != null && joinsNode.isArray()) {
                for (JsonNode joinNode : joinsNode) {
                    joins.add(new JoinRequest(
                            textValue(joinNode.get("schema")),
                            textValue(joinNode.get("table")),
                            textValue(joinNode.get("alias")),
                            joinNode.get("type") == null || joinNode.get("type").isNull() ? null
                                    : JoinType.fromValue(joinNode.get("type").asText()),
                            textValue(joinNode.get("sourceField")),
                            textValue(joinNode.get("targetField"))
                    ));
                }
            }
            FilterNode filtersNode = deserializeNode(parser, existsNode.get("filters"));
            return new ExistsFilterRequest(
                    textValue(existsNode.get("schema")),
                    textValue(existsNode.get("table")),
                    textValue(existsNode.get("alias")),
                    existsNode.get("type") == null || existsNode.get("type").isNull() ? JoinType.INNER
                            : JoinType.fromValue(existsNode.get("type").asText()),
                    textValue(existsNode.get("sourceField")),
                    textValue(existsNode.get("targetField")),
                    joins,
                    filtersNode == null ? FilterGroupRequest.empty() : filtersNode
            );
        }
        JsonNode operatorNode = node.get("operator");
        JsonNode valueNode = node.get("value");
        Object value = toJavaValue(valueNode);
        FilterRequest filter = new FilterRequest();
        filter.setField(textValue(node.get("field")));
        if (node.has("expression")) {
            filter.setExpression(ExpressionNodeDeserializer.deserializeNode(node.get("expression")));
        }
        filter.setOperator(operatorNode == null || operatorNode.isNull() ? null : FilterOperator.fromValue(operatorNode.asText()));
        filter.setValue(value);
        return filter;
    }

    private String textValue(JsonNode node) {
        return node == null || node.isNull() ? null : node.asText();
    }

    private Object toJavaValue(JsonNode node) {
        if (node == null || node.isNull()) {
            return null;
        }
        if (node.isArray()) {
            List<Object> values = new ArrayList<>();
            for (JsonNode child : node) {
                values.add(toJavaValue(child));
            }
            return values;
        }
        if (node.isObject()) {
            Map<String, Object> values = new LinkedHashMap<>();
            node.forEachEntry((name, value) -> values.put(name, toJavaValue(value)));
            return values;
        }
        if (node.isTextual()) {
            return node.asText();
        }
        if (node.isBoolean()) {
            return node.asBoolean();
        }
        if (node.isBigInteger()) {
            return node.bigIntegerValue();
        }
        if (node.isBigDecimal()) {
            return node.decimalValue();
        }
        if (node.isInt()) {
            return node.intValue();
        }
        if (node.isLong()) {
            return node.longValue();
        }
        if (node.isFloat() || node.isDouble()) {
            return node.doubleValue();
        }
        Number number = node.numberValue();
        if (number instanceof BigDecimal || number instanceof BigInteger) {
            return number;
        }
        return number;
    }
}
