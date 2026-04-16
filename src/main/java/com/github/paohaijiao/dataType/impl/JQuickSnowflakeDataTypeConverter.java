package com.github.paohaijiao.dataType.impl;

import com.github.paohaijiao.dataType.JQuickDataType;
import com.github.paohaijiao.dataType.JQuickDataTypeConverter;
import com.github.paohaijiao.dataType.enums.JQuickDataTypeFamily;

import java.util.Map;

/**
 * Snowflake 数据类型转换器
 */
public class JQuickSnowflakeDataTypeConverter implements JQuickDataTypeConverter {
    @Override
    public String convert(JQuickDataTypeFamily family, JQuickDataType dataType) {
        Map<String, Object> params = dataType.getParameters();
        switch (family) {
            case STRING:
                return "VARCHAR(" + getIntParam(params, "length", 255) + ")";
            case INTEGER:
                return "INTEGER";
            case LONG:
                return "BIGINT";
            case SHORT:
                return "SMALLINT";
            case FLOAT:
                return "FLOAT";
            case DOUBLE:
                return "DOUBLE";
            case DECIMAL:
                int p = getIntParam(params, "precision", 10);
                int s = getIntParam(params, "scale", 0);
                return "NUMBER(" + p + ", " + s + ")";
            case DATE:
                return "DATE";
            case TIME:
                return "TIME";
            case TIMESTAMP:
                return "TIMESTAMP_NTZ";
            case BOOLEAN:
                return "BOOLEAN";
            case JSON:
                return "VARIANT";
            default:
                return "VARCHAR(255)";
        }
    }

    private int getIntParam(Map<String, Object> params, String key, int defaultValue) {
        if (params != null && params.containsKey(key)) {
            Object v = params.get(key);
            if (v instanceof Number) return ((Number) v).intValue();
        }
        return defaultValue;
    }
}