package com.github.paohaijiao.dataType.impl;

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
import com.github.paohaijiao.dataType.JQuickDataType;
import com.github.paohaijiao.dataType.JQuickDataTypeConverter;
import com.github.paohaijiao.dataType.enums.JQuickDataTypeFamily;

import java.util.Map;

/**
 * H2 数据类型转换器
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/15
 */
public class JQuickH2DataTypeConverter implements JQuickDataTypeConverter {

    @Override
    public String convert(JQuickDataTypeFamily family, JQuickDataType dataType) {
        if (family == null) {
            return "VARCHAR(255)";
        }

        Map<String, Object> params = dataType.getParameters();

        switch (family) {
            case STRING:
                int length = getIntParam(params, "length", 255);
                if (length > Integer.MAX_VALUE) {
                    return "TEXT";
                }
                return "VARCHAR(" + length + ")";
            case CHAR:
                int charLength = getIntParam(params, "length", 1);
                return "CHAR(" + charLength + ")";
            case TEXT:
            case CLOB:
                return "TEXT";
            case INTEGER:
                return "INT";
            case LONG:
                return "BIGINT";
            case SHORT:
                return "SMALLINT";
            case BYTE:
                return "TINYINT";
            case FLOAT:
                return "REAL";
            case DOUBLE:
                return "DOUBLE";
            case DECIMAL:
                int precision = getIntParam(params, "precision", 10);
                int scale = getIntParam(params, "scale", 0);
                if (precision > 0 && scale >= 0) {
                    return "DECIMAL(" + precision + ", " + scale + ")";
                }
                return "DECIMAL";
            case DATE:
                return "DATE";
            case TIME:
                return "TIME";
            case TIMESTAMP:
                int tsPrecision = getIntParam(params, "precision", 6);
                if (tsPrecision > 0 && tsPrecision <= 9) {
                    return "TIMESTAMP(" + tsPrecision + ")";
                }
                return "TIMESTAMP";
            case BLOB:
                return "BLOB";
            case BINARY:
                int binaryLength = getIntParam(params, "length", 255);
                return "BINARY(" + binaryLength + ")";
            case VARBINARY:
                int varbinaryLength = getIntParam(params, "length", 255);
                return "VARBINARY(" + varbinaryLength + ")";
            case BOOLEAN:
                return "BOOLEAN";
            case JSON:
                return "JSON";
            default:
                return "VARCHAR(255)";
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