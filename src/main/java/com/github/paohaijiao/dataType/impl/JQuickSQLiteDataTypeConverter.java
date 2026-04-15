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
 * SQLite 数据类型转换器
 *
 * SQLite 使用动态类型系统，实际类型映射：
 * - INTEGER: 整数值
 * - REAL: 浮点值
 * - TEXT: 文本字符串
 * - BLOB: 二进制数据
 * - NUMERIC: 数值（灵活存储）
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/15
 */
public class JQuickSQLiteDataTypeConverter implements JQuickDataTypeConverter {

    @Override
    public String convert(JQuickDataTypeFamily family, JQuickDataType dataType) {
        if (family == null) {
            return "TEXT";
        }
        Map<String, Object> params = dataType.getParameters();
        switch (family) {
            case STRING:
            case CHAR:
            case TEXT:
            case CLOB:
                return "TEXT";

            case INTEGER:
                return "INTEGER";

            case LONG:
                return "INTEGER";

            case SHORT:
                return "INTEGER";

            case BYTE:
                return "INTEGER";

            case FLOAT:
                return "REAL";

            case DOUBLE:
                return "REAL";

            case DECIMAL:
                return "NUMERIC";

            case DATE:
            case TIME:
            case TIMESTAMP:
                return "NUMERIC";

            case BLOB:
            case BINARY:
            case VARBINARY:
                return "BLOB";

            case BOOLEAN:
                return "INTEGER";

            case JSON:
                return "TEXT";

            default:
                return "TEXT";
        }
    }
}