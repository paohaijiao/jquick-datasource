package com.github.paohaijiao.dataType.impl;


import com.github.paohaijiao.dataType.JQuickDataType;
import com.github.paohaijiao.dataType.JQuickDataTypeConverter;
import com.github.paohaijiao.dataType.enums.JQuickDataTypeFamily;

import java.util.Map;

/**
 * TDengine 数据类型转换器
 */
public class JQuickTDengineDataTypeConverter implements JQuickDataTypeConverter {
    @Override
    public String convert(JQuickDataTypeFamily family, JQuickDataType dataType) {
        Map<String, Object> params = dataType.getParameters();
        switch (family) {
            case STRING:
                return "BINARY(" + getIntParam(params, "length", 100) + ")";
            case INTEGER:
                return "INT";
            case LONG:
                return "BIGINT";
            case FLOAT:
                return "FLOAT";
            case DOUBLE:
                return "DOUBLE";
            case TIMESTAMP:
                return "TIMESTAMP";
            case BOOLEAN:
                return "BOOL";
            default:
                return "BINARY(100)";
        }
    }

    private int getIntParam(Map<String, Object> params, String key, int defaultValue) {
        if (params != null && params.containsKey(key)) {
            Object value = params.get(key);
            if (value instanceof Number) return ((Number) value).intValue();
        }
        return defaultValue;
    }
}
