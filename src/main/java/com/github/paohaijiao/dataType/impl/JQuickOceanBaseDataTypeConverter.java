package com.github.paohaijiao.dataType.impl;

import com.github.paohaijiao.dataType.JQuickDataType;
import com.github.paohaijiao.dataType.JQuickDataTypeConverter;
import com.github.paohaijiao.dataType.enums.JQuickDataTypeFamily;

import java.util.Map;

public class JQuickOceanBaseDataTypeConverter implements JQuickDataTypeConverter {

    private final String mode;

    public JQuickOceanBaseDataTypeConverter(String mode) {
        this.mode = mode;
    }

    @Override
    public String convert(JQuickDataTypeFamily family, JQuickDataType dataType) {
        if ("ORACLE".equals(mode)) {
            return convertOracleMode(family, dataType);
        }
        return convertMySQLMode(family, dataType);
    }

    private String convertMySQLMode(JQuickDataTypeFamily family, JQuickDataType dataType) {
        Map<String, Object> params = dataType.getParameters();
        switch (family) {
            case STRING:
                int length = getIntParam(params, "length", 255);
                return "VARCHAR(" + length + ")";
            case INTEGER:
                return "INT";
            case LONG:
                return "BIGINT";
            case SHORT:
                return "SMALLINT";
            case FLOAT:
                return "FLOAT";
            case DOUBLE:
                return "DOUBLE";
            case DECIMAL:
                int precision = getIntParam(params, "precision", 10);
                int scale = getIntParam(params, "scale", 0);
                return "DECIMAL(" + precision + ", " + scale + ")";
            case DATE:
                return "DATE";
            case TIMESTAMP:
                return "TIMESTAMP";
            case BOOLEAN:
                return "TINYINT(1)";
            default:
                return "VARCHAR(255)";
        }
    }

    private String convertOracleMode(JQuickDataTypeFamily family, JQuickDataType dataType) {
        Map<String, Object> params = dataType.getParameters();
        switch (family) {
            case STRING:
                int length = getIntParam(params, "length", 255);
                return "VARCHAR2(" + length + ")";
            case INTEGER:
                return "NUMBER(10)";
            case LONG:
                return "NUMBER(19)";
            case SHORT:
                return "NUMBER(5)";
            case FLOAT:
                return "FLOAT";
            case DOUBLE:
                return "DOUBLE";
            case DECIMAL:
                int precision = getIntParam(params, "precision", 10);
                int scale = getIntParam(params, "scale", 0);
                return "NUMBER(" + precision + ", " + scale + ")";
            case DATE:
                return "DATE";
            case TIMESTAMP:
                return "TIMESTAMP";
            case BOOLEAN:
                return "CHAR(1)";
            default:
                return "VARCHAR2(255)";
        }
    }

    private int getIntParam(Map<String, Object> params, String key, int defaultValue) {
        if (params != null && params.containsKey(key)) {
            Object value = params.get(key);
            if (value instanceof Number) {
                return ((Number) value).intValue();
            }
        }
        return defaultValue;
    }
}
