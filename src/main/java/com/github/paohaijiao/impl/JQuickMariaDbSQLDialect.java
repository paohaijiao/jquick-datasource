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
package com.github.paohaijiao.impl;

import com.github.paohaijiao.column.JQuickColumnDefinition;
import com.github.paohaijiao.dataType.JQuickDataTypeConverter;
import com.github.paohaijiao.dataType.impl.JQuickMariaDbSQLDataTypeConverter;
import com.github.paohaijiao.dataType.impl.JQuickMySQLDataTypeConverter;
import com.github.paohaijiao.dialect.JQuickAbsSQLDialect;
import com.github.paohaijiao.dialect.JQuickSQLDialect;
import com.github.paohaijiao.extra.JQuickIndexDefinition;
import com.github.paohaijiao.table.JQuickTableDefinition;

import java.util.Map;

/**
 * SQL方言抽象基类
 * 提供公共的流程和方法实现，子类可覆盖特定方法以适应不同数据库
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/13
 */
public  class JQuickMariaDbSQLDialect extends JQuickAbsSQLDialect implements JQuickSQLDialect {

    protected static final String MYSQL_QUOTE = "`";



    @Override
    public String getAutoIncrementKeyword() {
        return "AUTO_INCREMENT";
    }

    @Override
    protected JQuickDataTypeConverter createDataTypeConvert() {
        return new JQuickMariaDbSQLDataTypeConverter();
    }

    @Override
    protected String getQuoteKeyWord() {
        return MYSQL_QUOTE;
    }



    @Override
    protected void appendTableOptions(StringBuilder sql, JQuickTableDefinition table) {
        if (table.getComment() != null && !table.getComment().isEmpty()) {
            sql.append(" COMMENT = '").append(escapeString(table.getComment())).append("'");
        }
        if (table.getExtensions() != null && !table.getExtensions().isEmpty()) {
            for (Map.Entry<String, Object> entry : table.getExtensions().entrySet()) {
                if ( entry.getKey() == null) {
                    continue;
                }
                String key = entry.getKey().toLowerCase();
                Object value = entry.getValue();
                if (value == null) {
                    continue;
                }
                sql.append(" ").append(key).append("=").append(value);
            }
        }
    }


    @Override
    public String buildIndex(JQuickIndexDefinition index) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE ");
        if (index.isUnique()) {
            sb.append("UNIQUE ");
        }
        sb.append("INDEX ").append(quoteIdentifier(index.getIndexName())).append(" ");
        if (index.getType() != null) {
            switch (index.getType()) {
                case "BTREE":
                    sb.append("USING BTREE ");
                    break;
                case "HASH":
                    sb.append("USING HASH ");
                    break;
                case "FULLTEXT":
                    sb.append("FULLTEXT ");
                    break;
                default:
                    break;
            }
        }

        sb.append("ON ").append("${tableName}").append(" (");
        sb.append(formatColumnList(index.getColumns()));
        sb.append(")");
        if (index.getComment() != null && !index.getComment().isEmpty()) {
            sb.append(" COMMENT '").append(escapeString(index.getComment())).append("'");
        }

        return sb.toString();
    }

    @Override
    protected String getSpecialDefaultKeyword(String value) {
        if (value == null) {
            return null;
        }
        String upperValue = value.toUpperCase();
        if ("CURRENT_TIMESTAMP".equals(upperValue) || "NOW()".equals(upperValue)) {
            return "CURRENT_TIMESTAMP";
        }
        if ("CURRENT_DATE".equals(upperValue)) {
            return "CURRENT_DATE";
        }
        if ("CURRENT_TIME".equals(upperValue)) {
            return "CURRENT_TIME";
        }
        if ("CURRENT_TIMESTAMP(6)".equals(upperValue)) {
            return "CURRENT_TIMESTAMP(6)";
        }
        return null;
    }

    /**
     * 构建 ALTER TABLE MODIFY COLUMN 语句
     */
    public String buildModifyColumn(String tableName, JQuickColumnDefinition column) {
        return "ALTER TABLE " + quoteIdentifier(tableName) + " MODIFY " + buildColumnDefinition(column);
    }

    /**
     * 构建 ALTER TABLE CHANGE COLUMN 语句（重命名列）
     */
    public String buildChangeColumn(String tableName, String oldName, JQuickColumnDefinition newColumn) {
        return "ALTER TABLE " + quoteIdentifier(tableName) + " CHANGE " + quoteIdentifier(oldName) + " " + buildColumnDefinition(newColumn);
    }

    /**
     * 构建 SHOW CREATE TABLE 语句
     */
    public String buildShowCreateTable(String tableName) {
        return "SHOW CREATE TABLE " + quoteIdentifier(tableName);
    }

    /**
     * 构建 DESCRIBE 语句
     */
    public String buildDescribeTable(String tableName) {
        return "DESCRIBE " + quoteIdentifier(tableName);
    }
}
