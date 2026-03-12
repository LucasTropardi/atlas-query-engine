package br.com.lucast.atlas_query_engine.core.model;

import tools.jackson.core.JacksonException;
import tools.jackson.core.JsonParser;
import tools.jackson.databind.DeserializationContext;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.ValueDeserializer;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ExpressionNodeDeserializer extends ValueDeserializer<ExpressionNode> {

    @Override
    public ExpressionNode deserialize(JsonParser parser, DeserializationContext context) throws JacksonException {
        return deserializeNode(parser.readValueAsTree());
    }

    public static ExpressionNode deserializeNode(JsonNode node) {
        if (node == null || node.isNull()) {
            return new LiteralExpression(null);
        }
        if (node.isTextual()) {
            return new ColumnExpression(node.asText());
        }
        if (node.isNumber() || node.isBoolean()) {
            return new LiteralExpression(toJavaValue(node));
        }
        if (node.isObject() && node.has("column")) {
            return new ColumnExpression(textValue(node.get("column")));
        }
        if (node.isObject() && node.has("literal")) {
            return new LiteralExpression(toJavaValue(node.get("literal")));
        }
        if (node.isObject() && node.has("function")) {
            List<ExpressionNode> args = new ArrayList<>();
            JsonNode argsNode = node.get("args");
            if (argsNode != null && argsNode.isArray()) {
                for (JsonNode arg : argsNode) {
                    args.add(deserializeNode(arg));
                }
            }
            return new FunctionExpression(textValue(node.get("function")), args);
        }
        if (node.isObject() && node.has("operator")) {
            List<ExpressionNode> args = new ArrayList<>();
            JsonNode argsNode = node.get("args");
            if (argsNode != null && argsNode.isArray()) {
                for (JsonNode arg : argsNode) {
                    args.add(deserializeNode(arg));
                }
            }
            return new OperationExpression(textValue(node.get("operator")), args);
        }
        return new LiteralExpression(toJavaValue(node));
    }

    private static String textValue(JsonNode node) {
        return node == null || node.isNull() ? null : node.asText();
    }

    private static Object toJavaValue(JsonNode node) {
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
