package br.com.lucast.atlas_query_engine.core.validator;

import br.com.lucast.atlas_query_engine.core.exception.InvalidQueryException;
import br.com.lucast.atlas_query_engine.core.model.ColumnExpression;
import br.com.lucast.atlas_query_engine.core.model.ExpressionNode;
import br.com.lucast.atlas_query_engine.core.model.FunctionExpression;
import br.com.lucast.atlas_query_engine.core.model.LiteralExpression;
import br.com.lucast.atlas_query_engine.core.model.OperationExpression;
import java.util.Set;
import java.util.regex.Pattern;


final class ExpressionValidator {
    private static final Pattern SIMPLE_IDENTIFIER = Pattern.compile("[A-Za-z_][A-Za-z0-9_]*");
    private static final Pattern QUALIFIED_IDENTIFIER =
            Pattern.compile("[A-Za-z_][A-Za-z0-9_]*(\\.[A-Za-z_][A-Za-z0-9_]*){0,2}");
    private static final Set<String> ALLOWED_EXPRESSION_OPERATORS = Set.of("+", "-", "*", "/");

    static void validateRequiredSimpleIdentifier(String value, String label) {
        if (value == null || value.isBlank()) {
            throw new InvalidQueryException(label + " is required");
        }
        validateSimpleIdentifier(value, label);
    }

    static void validateSimpleIdentifier(String value, String label) {
        if (value == null || value.isBlank()) {
            return;
        }
        if (!SIMPLE_IDENTIFIER.matcher(value).matches()) {
            throw new InvalidQueryException("Invalid " + label + ": " + value);
        }
    }

    static void validateQualifiedIdentifier(String value, String label) {
        if (value == null || value.isBlank() || !QUALIFIED_IDENTIFIER.matcher(value).matches()) {
            throw new InvalidQueryException(label + " is invalid: " + value);
        }
    }

    static void validateExpression(ExpressionNode expression, String label) {
        if (expression instanceof ColumnExpression columnExpression) {
            validateQualifiedIdentifier(columnExpression.getColumn(), label + " column");
            return;
        }
        if (expression instanceof LiteralExpression) {
            return;
        }
        if (expression instanceof FunctionExpression functionExpression) {
            validateRequiredSimpleIdentifier(functionExpression.getFunction(), label + " function");
            if (functionExpression.getArgs() == null || functionExpression.getArgs().isEmpty()) {
                throw new InvalidQueryException(label + " function requires at least one argument");
            }
            functionExpression.getArgs().forEach(arg -> validateExpression(arg, label));
            return;
        }
        if (expression instanceof OperationExpression operationExpression) {
            if (operationExpression.getOperator() == null
                    || !ALLOWED_EXPRESSION_OPERATORS.contains(operationExpression.getOperator().trim())) {
                throw new InvalidQueryException(label + " operator is invalid: " + operationExpression.getOperator());
            }
            if (operationExpression.getArgs() == null || operationExpression.getArgs().size() < 2) {
                throw new InvalidQueryException(label + " operator requires at least two arguments");
            }
            operationExpression.getArgs().forEach(arg -> validateExpression(arg, label));
            return;
        }
        throw new InvalidQueryException(label + " is invalid");
    }
}
