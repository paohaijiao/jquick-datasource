package com.github.paohaijiao.dataType.impl;

import com.github.paohaijiao.dataType.JQuickDataType;
import com.github.paohaijiao.dataType.JQuickDataTypeConverter;
import com.github.paohaijiao.dataType.enums.JQuickDataTypeFamily;

import java.util.Map;

/**
 * Firebird 数据类型转换器
 */
public class JQuickFirebirdDataTypeConverter implements JQuickDataTypeConverter {
    @Override
    public String convert(JQuickDataTypeFamily family, JQuickDataType dataType) {
        Map<String, Object> params = dataType.getParameters();
        switch (family) {
            case STRING:
                int length = getIntParam(params, "length", 255);
                if (length > 32767) return "BLOB SUB_TYPE TEXT";
                return "VARCHAR(" + length + ")";
            case INTEGER:
                return "INTEGER";
            case LONG:
                return "BIGINT";
            case SHORT:
                return "SMALLINT";
            case FLOAT:
                return "FLOAT";
            case DOUBLE:
                return "DOUBLE PRECISION";
            case DECIMAL:
                int p = getIntParam(params, "precision", 10);
                int s = getIntParam(params, "scale", 0);
                return "DECIMAL(" + p + ", " + s + ")";
            case DATE:
                return "DATE";
            case TIMESTAMP:
                return "TIMESTAMP";
            case BOOLEAN:
                return "SMALLINT";
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

