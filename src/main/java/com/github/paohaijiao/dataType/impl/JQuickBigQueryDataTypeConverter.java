package com.github.paohaijiao.dataType.impl;


import com.github.paohaijiao.dataType.JQuickDataType;
import com.github.paohaijiao.dataType.JQuickDataTypeConverter;
import com.github.paohaijiao.dataType.enums.JQuickDataTypeFamily;

import java.util.Map;

/**
 * BigQuery 数据类型转换器
 */
public class JQuickBigQueryDataTypeConverter implements JQuickDataTypeConverter {

    @Override
    public String convert(JQuickDataTypeFamily family, JQuickDataType dataType) {
        if (family == null) {
            return "STRING";
        }

        Map<String, Object> params = dataType.getParameters();

        switch (family) {
            case STRING:
                return "STRING";

            case CHAR:
                return "STRING";

            case TEXT:
            case CLOB:
                return "STRING";

            case INTEGER:
                return "INT64";

            case LONG:
                return "INT64";

            case SHORT:
                return "INT64";

            case BYTE:
                return "INT64";

            case FLOAT:
                return "FLOAT64";

            case DOUBLE:
                return "FLOAT64";

            case DECIMAL:
                int precision = getIntParam(params, "precision", 38);
                int scale = getIntParam(params, "scale", 9);
                if (precision > 0 && scale >= 0) {
                    if (precision > 76) precision = 76;
                    return "NUMERIC(" + precision + ", " + scale + ")";
                }
                return "NUMERIC";

            case DATE:
                return "DATE";

            case TIME:
                return "TIME";

            case TIMESTAMP:
                return "TIMESTAMP";

            case BLOB:
            case BINARY:
            case VARBINARY:
                return "BYTES";

            case BOOLEAN:
                return "BOOL";

            case JSON:
                return "JSON";

            default:
                return "STRING";
        }
    }

    private int getIntParam(Map<String, Object> params, String key, int defaultValue) {
        if (params != null && params.containsKey(key)) {
            Object value = params.get(key);
            if (value instanceof Number) {
                return ((Number) value).intValue();
            }
            if (value instanceof String) {
                try {
                    return Integer.parseInt((String) value);
                } catch (NumberFormatException e) {
                    return defaultValue;
                }
            }
        }
        return defaultValue;
    }
}
