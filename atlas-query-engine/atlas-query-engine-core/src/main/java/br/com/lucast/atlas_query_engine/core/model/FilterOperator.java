package br.com.lucast.atlas_query_engine.core.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum FilterOperator {
    EQUALS("="),
    NOT_EQUALS("!="),
    GREATER_THAN(">"),
    GREATER_THAN_OR_EQUAL(">="),
    LESS_THAN("<"),
    LESS_THAN_OR_EQUAL("<="),
    LIKE("like"),
    IN("in"),
    BETWEEN("between");

    private final String value;

    FilterOperator(String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return value;
    }

    @JsonCreator
    public static FilterOperator fromValue(String value) {
        if (value == null) {
            return null;
        }
        for (FilterOperator operator : values()) {
            if (operator.value.equalsIgnoreCase(value.trim())) {
                return operator;
            }
        }
        throw new IllegalArgumentException("Unsupported filter operator: " + value);
    }
}
