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
 * SQL Server 数据类型转换器
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/15
 */
public class JQuickSQLServerDataTypeConverter implements JQuickDataTypeConverter {

    @Override
    public String convert(JQuickDataTypeFamily family, JQuickDataType dataType) {
        if (family == null) {
            return "NVARCHAR(255)";
        }
        Map<String, Object> params = dataType.getParameters();
        switch (family) {
            case STRING:
                int length = getIntParam(params, "length", 255);
                if (length > 4000) {
                    if (length > 1073741823) {
                        return "NVARCHAR(MAX)";
                    }
                    return "NVARCHAR(" + length + ")";
                }
                if (length == -1) {
                    return "NVARCHAR(MAX)";
                }
                return "NVARCHAR(" + length + ")";
            case CHAR:
                int charLength = getIntParam(params, "length", 1);
                if (charLength > 4000) {
                    return "NCHAR(4000)";
                }
                return "NCHAR(" + charLength + ")";
            case TEXT:
            case CLOB:
                return "NVARCHAR(MAX)";
            case INTEGER:
                return "INT";
            case LONG:
                return "BIGINT";
            case SHORT:
                return "SMALLINT";
            case BYTE:
                return "TINYINT";
            case FLOAT:
                return "FLOAT(24)";
            case DOUBLE:
                return "FLOAT(53)";
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
                int timePrecision = getIntParam(params, "precision", 0);
                if (timePrecision > 0 && timePrecision <= 7) {
                    return "TIME(" + timePrecision + ")";
                }
                return "TIME";
            case TIMESTAMP:
                int tsPrecision = getIntParam(params, "precision", 0);
                if (tsPrecision > 0 && tsPrecision <= 7) {
                    return "DATETIME2(" + tsPrecision + ")";
                }
                return "DATETIME2";
            case BLOB:
                return "VARBINARY(MAX)";
            case BINARY:
                int binaryLength = getIntParam(params, "length", 1);
                if (binaryLength > 8000) {
                    return "VARBINARY(MAX)";
                }
                return "BINARY(" + binaryLength + ")";
            case VARBINARY:
                int varbinaryLength = getIntParam(params, "length", 255);
                if (varbinaryLength > 8000) {
                    return "VARBINARY(MAX)";
                }
                return "VARBINARY(" + varbinaryLength + ")";
            case BOOLEAN:
                return "BIT";
            case JSON:
                return "NVARCHAR(MAX)";
            default:
                return "NVARCHAR(255)";
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
