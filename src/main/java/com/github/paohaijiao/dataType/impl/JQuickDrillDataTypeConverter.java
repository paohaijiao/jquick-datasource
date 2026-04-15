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
 * Apache Drill 数据类型转换器
 *
 * Drill 主要数据类型：
 * - 整数类型：TINYINT, SMALLINT, INT, BIGINT
 * - 浮点类型：FLOAT, DOUBLE, DECIMAL
 * - 字符串类型：VARCHAR, CHAR
 * - 日期时间：DATE, TIME, TIMESTAMP
 * - 复杂类型：ARRAY, MAP
 * - 其他：BOOLEAN, BINARY, INTERVAL
 *
 * Drill 支持动态类型，许多类型可以自动转换
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/15
 */
public class JQuickDrillDataTypeConverter implements JQuickDataTypeConverter {

    @Override
    public String convert(JQuickDataTypeFamily family, JQuickDataType dataType) {
        if (family == null) {
            return "VARCHAR";
        }

        Map<String, Object> params = dataType.getParameters();

        switch (family) {
            case STRING:
                int length = getIntParam(params, "length", 0);
                if (length > 0) {
                    return "VARCHAR(" + length + ")";
                }
                return "VARCHAR";

            case CHAR:
                int charLength = getIntParam(params, "length", 1);
                return "CHAR(" + charLength + ")";

            case TEXT:
            case CLOB:
                return "VARCHAR";

            case INTEGER:
                return "INT";

            case LONG:
                return "BIGINT";

            case SHORT:
                return "SMALLINT";

            case BYTE:
                return "TINYINT";

            case FLOAT:
                return "FLOAT";

            case DOUBLE:
                return "DOUBLE";

            case DECIMAL:
                int precision = getIntParam(params, "precision", 10);
                int scale = getIntParam(params, "scale", 0);
                if (precision > 0 && scale >= 0) {
                    if (precision > 38) {
                        precision = 38;
                    }
                    return "DECIMAL(" + precision + ", " + scale + ")";
                }
                return "DECIMAL(10, 0)";

            case DATE:
                return "DATE";

            case TIME:
                int timePrecision = getIntParam(params, "precision", 0);
                if (timePrecision > 0 && timePrecision <= 9) {
                    return "TIME(" + timePrecision + ")";
                }
                return "TIME";

            case TIMESTAMP:
                int tsPrecision = getIntParam(params, "precision", 0);
                if (tsPrecision > 0 && tsPrecision <= 9) {
                    return "TIMESTAMP(" + tsPrecision + ")";
                }
                return "TIMESTAMP";

            case BLOB:
            case BINARY:
            case VARBINARY:
                return "BINARY";

            case BOOLEAN:
                return "BOOLEAN";

            case JSON:
                return "VARCHAR";

            default:
                return "VARCHAR";
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