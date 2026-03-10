package br.com.lucast.atlas_query_engine.core.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum SortDirection {
    ASC("asc"),
    DESC("desc");

    private final String value;

    SortDirection(String value) {
        this.value = value;
    }

    @JsonValue
    public String getValue() {
        return value;
    }

    @JsonCreator
    public static SortDirection fromValue(String value) {
        if (value == null) {
            return null;
        }
        for (SortDirection direction : values()) {
            if (direction.value.equalsIgnoreCase(value.trim())) {
                return direction;
            }
        }
        throw new IllegalArgumentException("Unsupported sort direction: " + value);
    }
}
