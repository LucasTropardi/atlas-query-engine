package br.com.lucast.atlas_query_engine.core.model;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

public enum MetricOperation {
    COUNT("count", "COUNT"),
    SUM("sum", "SUM"),
    AVG("avg", "AVG"),
    MIN("min", "MIN"),
    MAX("max", "MAX");

    private final String value;
    private final String sqlFunction;

    MetricOperation(String value, String sqlFunction) {
        this.value = value;
        this.sqlFunction = sqlFunction;
    }

    @JsonValue
    public String getValue() {
        return value;
    }

    public String getSqlFunction() {
        return sqlFunction;
    }

    @JsonCreator
    public static MetricOperation fromValue(String value) {
        if (value == null) {
            return null;
        }
        for (MetricOperation operation : values()) {
            if (operation.value.equalsIgnoreCase(value.trim())) {
                return operation;
            }
        }
        throw new IllegalArgumentException("Unsupported metric operation: " + value);
    }
}
