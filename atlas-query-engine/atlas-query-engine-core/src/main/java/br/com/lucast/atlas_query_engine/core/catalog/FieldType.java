package br.com.lucast.atlas_query_engine.core.catalog;

import br.com.lucast.atlas_query_engine.core.exception.InvalidQueryException;
import java.math.BigDecimal;
import java.time.LocalDate;

public enum FieldType {
    STRING {
        @Override
        public Object convert(Object value) {
            return value == null ? null : String.valueOf(value);
        }
    },
    LONG {
        @Override
        public Object convert(Object value) {
            if (value instanceof Number number) {
                return number.longValue();
            }
            return Long.valueOf(String.valueOf(value));
        }
    },
    BOOLEAN {
        @Override
        public Object convert(Object value) {
            if (value instanceof Boolean bool) {
                return bool;
            }
            if (value instanceof Number number) {
                return number.intValue() != 0;
            }
            String normalized = String.valueOf(value).trim();
            if ("true".equalsIgnoreCase(normalized) || "1".equals(normalized)) {
                return true;
            }
            if ("false".equalsIgnoreCase(normalized) || "0".equals(normalized)) {
                return false;
            }
            throw new IllegalArgumentException("Unsupported boolean value: " + value);
        }
    },
    DECIMAL {
        @Override
        public Object convert(Object value) {
            if (value instanceof BigDecimal decimal) {
                return decimal;
            }
            if (value instanceof Number number) {
                return BigDecimal.valueOf(number.doubleValue());
            }
            return new BigDecimal(String.valueOf(value));
        }
    },
    DATE {
        @Override
        public Object convert(Object value) {
            if (value instanceof LocalDate localDate) {
                return localDate;
            }
            return LocalDate.parse(String.valueOf(value));
        }
    };

    public abstract Object convert(Object value);

    public Object safeConvert(Object value, String fieldName) {
        try {
            return convert(value);
        } catch (RuntimeException exception) {
            throw new InvalidQueryException("Invalid value for field " + fieldName + ": " + value);
        }
    }
}
