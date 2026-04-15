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
 * Oracle 数据类型转换器
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/15
 */
public class JQuickOracleDataTypeConverter implements JQuickDataTypeConverter {

    @Override
    public String convert(JQuickDataTypeFamily family, JQuickDataType dataType) {
        if (family == null) {
            return "VARCHAR2(255)";
        }
        Map<String, Object> params = dataType.getParameters();
        switch (family) {
            case STRING:
                int length = getIntParam(params, "length", 255);
                if (length > 4000) {
                    return "CLOB";
                }
                return "VARCHAR2(" + length + ")";
            case CHAR:
                int charLength = getIntParam(params, "length", 1);
                return "CHAR(" + charLength + ")";
            case TEXT:
            case CLOB:
                return "CLOB";
            case INTEGER:
                return "NUMBER(10)";
            case LONG:
                return "NUMBER(19)";
            case SHORT:
                return "NUMBER(5)";
            case BYTE:
                return "NUMBER(3)";
            case FLOAT:
                return "BINARY_FLOAT";
            case DOUBLE:
                return "BINARY_DOUBLE";
            case DECIMAL:
                int precision = getIntParam(params, "precision", 10);
                int scale = getIntParam(params, "scale", 0);
                if (precision > 0 && scale >= 0) {
                    return "NUMBER(" + precision + ", " + scale + ")";
                }
                return "NUMBER";
            case DATE:
                return "DATE";
            case TIME:
                return "TIMESTAMP";
            case TIMESTAMP:
                int tsPrecision = getIntParam(params, "precision", 6);
                if (tsPrecision > 0 && tsPrecision <= 9) {
                    return "TIMESTAMP(" + tsPrecision + ")";
                }
                return "TIMESTAMP";
            case BLOB:
                return "BLOB";
            case BINARY:
            case VARBINARY:
                int binaryLength = getIntParam(params, "length", 2000);
                if (binaryLength <= 2000) {
                    return "RAW(" + binaryLength + ")";
                }
                return "BLOB";
            case BOOLEAN:
                return "NUMBER(1)";

            case JSON:
                // Oracle 12c+ 支持 JSON
                return "JSON";

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
