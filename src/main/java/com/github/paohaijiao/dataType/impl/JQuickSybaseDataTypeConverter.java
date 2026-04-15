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
 * Sybase 数据类型转换器
 *
 * Sybase ASE 主要数据类型：
 * - CHAR/VARCHAR
 * - INT/SMALLINT/TINYINT/BIGINT
 * - FLOAT/DOUBLE/REAL
 * - DECIMAL/NUMERIC
 * - DATE/TIME/DATETIME/SMALLDATETIME
 * - IMAGE/TEXT
 * - BIT
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/15
 */
public class JQuickSybaseDataTypeConverter implements JQuickDataTypeConverter {

    @Override
    public String convert(JQuickDataTypeFamily family, JQuickDataType dataType) {
        if (family == null) {
            return "VARCHAR(255)";
        }

        Map<String, Object> params = dataType.getParameters();

        switch (family) {
            case STRING:
                int length = getIntParam(params, "length", 255);
                if (length > 16384) {
                    return "TEXT";
                }
                if (length > 255) {
                    return "VARCHAR(" + length + ")";
                }
                return "VARCHAR(" + length + ")";

            case CHAR:
                int charLength = getIntParam(params, "length", 1);
                if (charLength > 16384) {
                    return "TEXT";
                }
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
                return "FLOAT";
            case DECIMAL:
                int precision = getIntParam(params, "precision", 18);
                int scale = getIntParam(params, "scale", 0);
                if (precision > 0 && scale >= 0) {
                    if (precision > 38) {
                        precision = 38;
                    }
                    return "DECIMAL(" + precision + ", " + scale + ")";
                }
                return "DECIMAL(18, 0)";
            case DATE:
                return "DATE";
            case TIME:
                return "TIME";
            case TIMESTAMP:
                return "DATETIME";
            case BLOB:
                return "IMAGE";
            case BINARY:
            case VARBINARY:
                int binaryLength = getIntParam(params, "length", 255);
                if (binaryLength > 16384) {
                    return "IMAGE";
                }
                return "VARBINARY(" + binaryLength + ")";
            case BOOLEAN:
                return "BIT";
            case JSON:
                // Sybase 16+ 支持 JSON
                return "VARCHAR(MAX)";

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