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
 *
 * Copyright (c) [2025-2099] Martin (goudingcheng@gmail.com)
 */
package com.github.paohaijiao.dataType.impl;

import com.github.paohaijiao.dataType.JQuickDataType;
import com.github.paohaijiao.dataType.JQuickDataTypeConverter;
import com.github.paohaijiao.dataType.enums.JQuickDataTypeFamily;

/**
 * Apache Phoenix 数据类型转换器
 *
 * Phoenix 数据类型映射：
 * - VARCHAR/CHAR -> VARCHAR/CHAR
 * - INTEGER -> INTEGER
 * - LONG -> BIGINT
 * - DECIMAL -> DECIMAL(precision, scale)
 * - DATE -> DATE
 * - TIME -> TIME
 * - TIMESTAMP -> TIMESTAMP
 * - BOOLEAN -> BOOLEAN
 * - BLOB/BINARY -> VARBINARY
 * - JSON -> VARCHAR (Phoenix 不原生支持 JSON)
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/16
 */
public class JQuickPhoenixDataTypeConverter implements JQuickDataTypeConverter {

    private int getIntParameter(JQuickDataType dataType, String key, int defaultValue) {
        if (dataType.getParameters() != null && dataType.getParameters().containsKey(key)) {
            Object value = dataType.getParameters().get(key);
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

    @Override
    public String convert(JQuickDataTypeFamily family, JQuickDataType dataType) {
        switch (family) {
            case STRING:
                int length = getIntParameter(dataType, "length", 255);
                return "VARCHAR(" + length + ")";
            case CHAR:
                int charLength = getIntParameter(dataType, "length", 1);
                return "CHAR(" + charLength + ")";
            case TEXT:
            case CLOB:
                // Phoenix 使用 VARCHAR 存储大文本，最大 2GB
                return "VARCHAR";
            case BYTE:
                return "TINYINT";
            case SHORT:
                return "SMALLINT";
            case INTEGER:
                return "INTEGER";
            case LONG:
                return "BIGINT";
            case FLOAT:
                return "FLOAT";
            case DOUBLE:
                return "DOUBLE";
            case DECIMAL:
                int precision = getIntParameter(dataType, "precision", 10);
                int scale = getIntParameter(dataType, "scale", 0);
                return "DECIMAL(" + precision + "," + scale + ")";
            case DATE:
                return "DATE";
            case TIME:
                return "TIME";
            case TIMESTAMP:
                return "TIMESTAMP";
            case BLOB:
            case BINARY:
            case VARBINARY:
                return "VARBINARY";
            case BOOLEAN:
                return "BOOLEAN";
            case JSON:
                // Phoenix 不原生支持 JSON，转为 VARCHAR
                return "VARCHAR";
            default:
                return "VARCHAR(255)";
        }
    }
}
