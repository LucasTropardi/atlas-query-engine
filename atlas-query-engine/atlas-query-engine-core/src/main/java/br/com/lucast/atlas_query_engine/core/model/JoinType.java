package br.com.lucast.atlas_query_engine.core.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum JoinType {
    INNER("INNER"),
    LEFT("LEFT");

    private final String value;

    JoinType(String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return value;
    }

    @JsonCreator
    public static JoinType fromValue(String value) {
        if (value == null) {
            return null;
        }
        for (JoinType type : values()) {
            if (type.value.equalsIgnoreCase(value.trim())) {
                return type;
            }
        }
        throw new IllegalArgumentException("Unsupported join type: " + value);
    }
}
