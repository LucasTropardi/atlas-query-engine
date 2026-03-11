package br.com.lucast.atlas_query_engine.core.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum LogicalOperator {
    AND,
    OR;

    @JsonValue
    public String getValue() {
        return name();
    }

    @JsonCreator
    public static LogicalOperator fromValue(String value) {
        if (value == null) {
            return null;
        }
        for (LogicalOperator operator : values()) {
            if (operator.name().equalsIgnoreCase(value.trim())) {
                return operator;
            }
        }
        throw new IllegalArgumentException("Unsupported logical operator: " + value);
    }
}
