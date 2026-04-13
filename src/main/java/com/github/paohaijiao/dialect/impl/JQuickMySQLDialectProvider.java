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
package com.github.paohaijiao.dialect.impl;

import com.github.paohaijiao.column.JQuickColumnDefinition;
import com.github.paohaijiao.dataType.JQuickDataType;
import com.github.paohaijiao.dataType.enums.JQuickDataTypeFamily;
import com.github.paohaijiao.dialect.JQuickAbsSQLDialect;
import com.github.paohaijiao.extra.JQuickIndexDefinition;
import com.github.paohaijiao.table.JQuickTableDefinition;

/**
 * MySQL 方言实现
 * 继承 AbsSQLDialect，复用通用逻辑
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/13
 */
public class JQuickMySQLDialectProvider extends JQuickAbsSQLDialect {

    protected static final String MYSQL_QUOTE = "`";

    @Override
    protected String quoteIdentifier(String identifier) {
        if (identifier == null || identifier.isEmpty()) {
            return identifier;
        }
        if (identifier.startsWith(MYSQL_QUOTE) && identifier.endsWith(MYSQL_QUOTE)) {
            return identifier;
        }
        return MYSQL_QUOTE + identifier + MYSQL_QUOTE;
    }

    @Override
    protected String convertDataType(JQuickDataTypeFamily family, JQuickDataType dataType) {
        switch (family) {
            case VARCHAR:
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
            case INT:
                return "INT";
            case BIGINT:
                return "BIGINT";
            case SMALLINT:
                return "SMALLINT";
            case TINYINT:
                return "TINYINT";
            case DECIMAL:
                int precision = getIntParameter(dataType, "precision", 10);
                int scale = getIntParameter(dataType, "scale", 0);
                return "DECIMAL(" + precision + "," + scale + ")";
            case FLOAT:
                return "FLOAT";
            case DOUBLE:
                return "DOUBLE";
            case DATE:
                return "DATE";
            case TIME:
                return "TIME";
            case TIMESTAMP:
                int timestampPrecision = getIntParameter(dataType, "precision", 0);
                return timestampPrecision > 0 ? "TIMESTAMP(" + timestampPrecision + ")" : "TIMESTAMP";
            case DATETIME:
                int datetimePrecision = getIntParameter(dataType, "precision", 0);
                return datetimePrecision > 0 ? "DATETIME(" + datetimePrecision + ")" : "DATETIME";
            case BLOB:
                String blobType = getStringParameter(dataType, "blobType", "BLOB");
                return blobType;
            case BINARY:
                int binaryLength = getIntParameter(dataType, "length", 1);
                return "BINARY(" + binaryLength + ")";
            case VARBINARY:
                int varbinaryLength = getIntParameter(dataType, "length", 255);
                return "VARBINARY(" + varbinaryLength + ")";
            case BOOLEAN:
                return "BOOLEAN";
            case JSON:
                return "JSON";
            case OTHER:
            default:
                return "VARCHAR(255)";
        }
    }

    @Override
    public String getAutoIncrementKeyword() {
        return "AUTO_INCREMENT";
    }

    @Override
    protected void appendTableOptions(StringBuilder sql, JQuickTableDefinition table) {
        super.appendTableOptions(sql, table);

        // MySQL 特定选项
        sql.append(" ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci");

        // 处理 extensions 中的额外选项
        if (table.getExtensions() != null) {
            if (table.getExtensions().containsKey("engine")) {
                String engine = table.getExtensions().get("engine").toString();
                // 替换默认引擎
                int engineStart = sql.indexOf(" ENGINE=");
                int engineEnd = sql.indexOf(" ", engineStart + 8);
                if (engineEnd == -1) {
                    engineEnd = sql.length();
                }
                sql.replace(engineStart, engineEnd, " ENGINE=" + engine);
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
                case BTREE:
                    sb.append("USING BTREE ");
                    break;
                case HASH:
                    sb.append("USING HASH ");
                    break;
                case FULLTEXT:
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