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
 * Microsoft Access 数据类型转换器
 *
 * Access 主要数据类型：
 * - TEXT (Short Text, 最多255字符)
 * - LONGTEXT (Long Text, 以前叫 MEMO)
 * - NUMBER (Byte/Integer/Long/Single/Double/Decimal)
 * - CURRENCY
 * - DATE/TIME
 * - YES/NO (Boolean)
 * - AUTOINCREMENT
 * - OLE OBJECT
 * - HYPERLINK
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/15
 */
public class JQuickAccessDataTypeConverter implements JQuickDataTypeConverter {

    @Override
    public String convert(JQuickDataTypeFamily family, JQuickDataType dataType) {
        if (family == null) {
            return "TEXT(255)";
        }
        Map<String, Object> params = dataType.getParameters();
        switch (family) {
            case STRING:
                int length = getIntParam(params, "length", 255);
                if (length > 255) {
                    return "LONGTEXT";
                }
                if (length == -1) {
                    return "LONGTEXT";
                }
                return "TEXT(" + length + ")";

            case CHAR:
                int charLength = getIntParam(params, "length", 1);
                if (charLength > 255) {
                    return "LONGTEXT";
                }
                return "TEXT(" + charLength + ")";

            case TEXT:
            case CLOB:
                return "LONGTEXT";

            case INTEGER:
                return "LONG";

            case LONG:
                return "LONG";

            case SHORT:
                return "INTEGER";

            case BYTE:
                return "BYTE";

            case FLOAT:
                return "SINGLE";

            case DOUBLE:
                return "DOUBLE";

            case DECIMAL:
                int precision = getIntParam(params, "precision", 10);
                int scale = getIntParam(params, "scale", 0);
                if (precision > 0 && scale >= 0) {
                    if (precision <= 28) {
                        return "DECIMAL(" + precision + ", " + scale + ")";
                    }
                }
                return "CURRENCY";

            case DATE:
                return "DATETIME";

            case TIME:
                return "DATETIME";

            case TIMESTAMP:
                return "DATETIME";

            case BLOB:
                return "OLEOBJECT";

            case BINARY:
            case VARBINARY:
                return "OLEOBJECT";

            case BOOLEAN:
                return "YESNO";

            case JSON:
                // Access 不支持 JSON，使用 LONGTEXT
                return "LONGTEXT";

            default:
                return "TEXT(255)";
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
