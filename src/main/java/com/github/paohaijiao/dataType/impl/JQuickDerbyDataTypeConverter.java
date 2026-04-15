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
 * Apache Derby 数据类型转换器
 *
 * Derby 主要数据类型：
 * - CHAR/VARCHAR/LONG VARCHAR
 * - CLOB
 * - INTEGER/SMALLINT/BIGINT
 * - FLOAT/DOUBLE/REAL
 * - DECIMAL/NUMERIC
 * - DATE/TIME/TIMESTAMP
 * - BLOB
 * - BOOLEAN
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/15
 */
public class JQuickDerbyDataTypeConverter implements JQuickDataTypeConverter {

    @Override
    public String convert(JQuickDataTypeFamily family, JQuickDataType dataType) {
        if (family == null) {
            return "VARCHAR(255)";
        }

        Map<String, Object> params = dataType.getParameters();

        switch (family) {
            case STRING:
                int length = getIntParam(params, "length", 255);
                if (length > 32700) {
                    return "LONG VARCHAR";
                }
                if (length > 255) {
                    return "VARCHAR(" + length + ")";
                }
                return "VARCHAR(" + length + ")";

            case CHAR:
                int charLength = getIntParam(params, "length", 1);
                if (charLength > 254) {
                    return "LONG VARCHAR";
                }
                return "CHAR(" + charLength + ")";

            case TEXT:
            case CLOB:
                return "CLOB";

            case INTEGER:
                return "INTEGER";

            case LONG:
                return "BIGINT";

            case SHORT:
                return "SMALLINT";

            case BYTE:
                return "SMALLINT";

            case FLOAT:
                return "REAL";

            case DOUBLE:
                return "DOUBLE";

            case DECIMAL:
                int precision = getIntParam(params, "precision", 10);
                int scale = getIntParam(params, "scale", 0);
                if (precision > 0 && scale >= 0) {
                    if (precision > 31) {
                        precision = 31;
                    }
                    return "DECIMAL(" + precision + ", " + scale + ")";
                }
                return "DECIMAL(10, 0)";

            case DATE:
                return "DATE";

            case TIME:
                return "TIME";

            case TIMESTAMP:
                return "TIMESTAMP";

            case BLOB:
                int blobLength = getIntParam(params, "length", 1048576);
                if (blobLength > 2147483647) {
                    return "BLOB";
                }
                return "BLOB(" + blobLength + ")";

            case BINARY:
            case VARBINARY:
                int binaryLength = getIntParam(params, "length", 255);
                if (binaryLength > 32700) {
                    return "BLOB";
                }
                return "VARCHAR(" + binaryLength + ") FOR BIT DATA";

            case BOOLEAN:
                return "BOOLEAN";

            case JSON:
                // Derby 不支持 JSON，使用 CLOB
                return "CLOB";

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
