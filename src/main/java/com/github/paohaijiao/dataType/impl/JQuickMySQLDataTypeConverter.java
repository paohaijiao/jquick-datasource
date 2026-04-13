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

/**
 * packageName com.github.paohaijiao.dataType.impl
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/13
 */

import com.github.paohaijiao.dataType.JQuickDataType;
import com.github.paohaijiao.dataType.JQuickDataTypeConverter;
import com.github.paohaijiao.dataType.enums.JQuickDataTypeFamily;

/**
 * MySQL 数据类型转换器
 */
public  class JQuickMySQLDataTypeConverter implements JQuickDataTypeConverter {

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

    private String getStringParameter(JQuickDataType dataType, String key, String defaultValue) {
        if (dataType.getParameters() != null && dataType.getParameters().containsKey(key)) {
            Object value = dataType.getParameters().get(key);
            if (value != null) {
                return value.toString();
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
                String textType = getStringParameter(dataType, "textType", "TEXT");
                return textType;
            case CLOB:
                return "LONGTEXT";
            case BYTE:
                return "TINYINT";
            case SHORT:
                return "SMALLINT";
            case INTEGER:
                return "INT";
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
                int tsPrecision = getIntParameter(dataType, "precision", 0);
                return tsPrecision > 0 ? "TIMESTAMP(" + tsPrecision + ")" : "TIMESTAMP";
            case BLOB:
                String blobType = getStringParameter(dataType, "blobType", "BLOB");
                return blobType;
            case BINARY:
                int binLength = getIntParameter(dataType, "length", 1);
                return "BINARY(" + binLength + ")";
            case VARBINARY:
                int varBinLength = getIntParameter(dataType, "length", 255);
                return "VARBINARY(" + varBinLength + ")";
            case BOOLEAN:
                return "BOOLEAN";
            case JSON:
                return "JSON";
            default:
                return "VARCHAR(255)";
        }
    }
}