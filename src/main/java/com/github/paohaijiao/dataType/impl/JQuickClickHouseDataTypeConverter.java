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

import com.github.paohaijiao.dataType.JQuickDataType;
import com.github.paohaijiao.dataType.JQuickDataTypeConverter;
import com.github.paohaijiao.dataType.enums.JQuickDataTypeFamily;

import java.util.Map;

/**
 * ClickHouse 数据类型转换器
 *
 * ClickHouse 主要数据类型：
 * - 整数类型：UInt8, UInt16, UInt32, UInt64, Int8, Int16, Int32, Int64
 * - 浮点类型：Float32, Float64, Decimal
 * - 字符串类型：String, FixedString
 * - 日期时间：Date, DateTime, DateTime64
 * - 复杂类型：Array, Tuple, Map, Nested
 * - 特殊类型：Nullable, LowCardinality
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/15
 */
public class JQuickClickHouseDataTypeConverter implements JQuickDataTypeConverter {

    @Override
    public String convert(JQuickDataTypeFamily family, JQuickDataType dataType) {
        if (family == null) {
            return "String";
        }

        Map<String, Object> params = dataType.getParameters();

        // 是否可为空
        boolean nullable = true;
        if (params != null && params.containsKey("nullable")) {
            nullable = (Boolean) params.get("nullable");
        }

        // 是否使用 LowCardinality 优化
        boolean lowCardinality = false;
        if (params != null && params.containsKey("lowCardinality")) {
            lowCardinality = (Boolean) params.get("lowCardinality");
        }

        String baseType;

        switch (family) {
            case STRING:
                int length = getIntParam(params, "length", 0);
                if (length > 0) {
                    baseType = "FixedString(" + length + ")";
                } else {
                    baseType = "String";
                }
                break;

            case CHAR:
                int charLength = getIntParam(params, "length", 1);
                baseType = "FixedString(" + charLength + ")";
                break;

            case TEXT:
            case CLOB:
                baseType = "String";
                break;

            case INTEGER:
                baseType = "Int32";
                break;

            case LONG:
                baseType = "Int64";
                break;

            case SHORT:
                baseType = "Int16";
                break;

            case BYTE:
                baseType = "Int8";
                break;

            case FLOAT:
                baseType = "Float32";
                break;

            case DOUBLE:
                baseType = "Float64";
                break;

            case DECIMAL:
                int precision = getIntParam(params, "precision", 10);
                int scale = getIntParam(params, "scale", 0);
                if (precision > 0 && scale >= 0) {
                    if (precision > 76) {
                        precision = 76;
                    }
                    baseType = "Decimal(" + precision + ", " + scale + ")";
                } else {
                    baseType = "Decimal(10, 0)";
                }
                break;

            case DATE:
                baseType = "Date";
                break;

            case TIME:
                baseType = "DateTime";
                break;

            case TIMESTAMP:
                int tsPrecision = getIntParam(params, "precision", 3);
                if (tsPrecision > 0 && tsPrecision <= 9) {
                    baseType = "DateTime64(" + tsPrecision + ")";
                } else {
                    baseType = "DateTime";
                }
                break;

            case BLOB:
            case BINARY:
            case VARBINARY:
                baseType = "String";
                break;

            case BOOLEAN:
                baseType = "UInt8";
                break;

            case JSON:
                baseType = "String";
                break;

            default:
                baseType = "String";
        }

        // 应用 LowCardinality 优化
        if (lowCardinality && (baseType.equals("String") || baseType.startsWith("FixedString"))) {
            baseType = "LowCardinality(" + baseType + ")";
        }

        // 应用 Nullable
        if (nullable && !baseType.startsWith("Nullable")) {
            return "Nullable(" + baseType + ")";
        }

        return baseType;
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