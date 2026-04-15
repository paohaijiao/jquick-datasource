package com.github.paohaijiao.dataType.impl;

import com.github.paohaijiao.dataType.JQuickDataType;
import com.github.paohaijiao.dataType.JQuickDataTypeConverter;
import com.github.paohaijiao.dataType.enums.JQuickDataTypeFamily;

/**
 * InfluxDB 数据类型转换器
 */
public class JQuickInfluxDBDataTypeConverter implements JQuickDataTypeConverter {
    @Override
    public String convert(JQuickDataTypeFamily family, JQuickDataType dataType) {
        switch (family) {
            case FLOAT: return "float";
            case INTEGER: return "integer";
            case LONG: return "integer";
            case STRING: return "string";
            case BOOLEAN: return "boolean";
            case TIMESTAMP: return "timestamp";
            default: return "string";
        }
    }
}