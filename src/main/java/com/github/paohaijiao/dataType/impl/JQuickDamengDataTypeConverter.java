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
 * 达梦数据库数据类型转换器
 *
 * 达梦数据库主要数据类型：
 * - CHAR/VARCHAR/VARCHAR2
 * - NUMBER/DECIMAL
 * - INT/BIGINT/SMALLINT/TINYINT
 * - DATE/TIME/DATETIME/TIMESTAMP
 * - BLOB/CLOB
 * - BIT/BOOLEAN
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/15
 */
public class JQuickDamengDataTypeConverter implements JQuickDataTypeConverter {

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
                if (length > 8000) {
                    return "CLOB";
                }
                return "VARCHAR2(" + length + ")";

            case CHAR:
                int charLength = getIntParam(params, "length", 1);
                if (charLength > 8000) {
                    return "CLOB";
                }
                return "CHAR(" + charLength + ")";
            case TEXT:
            case CLOB:
                return "CLOB";
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
                return "DECIMAL";
            case DATE:
                return "DATE";
            case TIME:
                int timePrecision = getIntParam(params, "precision", 0);
                if (timePrecision > 0 && timePrecision <= 6) {
                    return "TIME(" + timePrecision + ")";
                }
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
            case VARBINARY:
                int binaryLength = getIntParam(params, "length", 4000);
                if (binaryLength > 4000) {
                    return "BLOB";
                }
                return "VARBINARY(" + binaryLength + ")";
            case BOOLEAN:
                return "BIT";
            case JSON:
                // 达梦支持 JSON 类型
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
