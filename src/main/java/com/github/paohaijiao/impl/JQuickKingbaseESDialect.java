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
import com.github.paohaijiao.dataType.JQuickDataType;
import com.github.paohaijiao.dataType.JQuickDataTypeConverter;
import com.github.paohaijiao.dataType.impl.JQuickKingbaseESDataTypeConverter;
import com.github.paohaijiao.dialect.JQuickAbsSQLDialect;
import com.github.paohaijiao.enums.JQuickForeignKeyAction;
import com.github.paohaijiao.extra.JQuickIndexDefinition;
import com.github.paohaijiao.table.JQuickTableDefinition;

/**
 * KingbaseES（人大金仓）方言实现
 * KingbaseES 兼容 PostgreSQL 语法
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/13
 */
public class JQuickKingbaseESDialect extends JQuickAbsSQLDialect {

    protected static final String KINGBASE_QUOTE = "\"";

    private static final String SEQ_SUFFIX = "_seq";

    @Override
    protected JQuickDataTypeConverter createDataTypeConvert() {
        return new JQuickKingbaseESDataTypeConverter();
    }

    @Override
    protected String getQuoteKeyWord() {
        return KINGBASE_QUOTE;
    }



    @Override
    public String getAutoIncrementKeyword() {
        return "SERIAL";
    }

    @Override
    protected void appendNullClause(StringBuilder def, JQuickColumnDefinition column) {
        if (!column.isNullable()) {
            def.append(" NOT NULL");
        }
    }

    @Override
    protected void appendColumnComment(StringBuilder def, JQuickColumnDefinition column) {
        // KingbaseES 的列注释需要使用 COMMENT ON 语句单独添加
        // 不在列定义中内联注释
        // 注释将在 buildTableComments 中单独处理
    }

    @Override
    protected void appendTableOptions(StringBuilder sql, JQuickTableDefinition table) {
        // KingbaseES 表注释需要单独处理
        if (table.getExtensions() != null) {
            if (table.getExtensions().containsKey("tablespace")) {
                String tablespace = table.getExtensions().get("tablespace").toString();
                sql.append(" TABLESPACE ").append(quoteIdentifier(tablespace));
            }
        }
    }

    @Override
    protected void appendDefaultClause(StringBuilder def, JQuickColumnDefinition column) {
        // 对于自增列，KingbaseES 使用 SERIAL 类型，不需要 DEFAULT
        if (column.isAutoIncrement()) {
            return;
        }
        super.appendDefaultClause(def, column);
    }

    @Override
    protected String formatBooleanDefault(boolean value) {
        return value ? "TRUE" : "FALSE";
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
        if ("CURRENT_USER".equals(upperValue)) {
            return "CURRENT_USER";
        }
        if ("CURRENT_TIME".equals(upperValue)) {
            return "CURRENT_TIME";
        }
        return null;
    }

    @Override
    protected String convertForeignKeyAction(JQuickForeignKeyAction action) {
        if (action == null) {
            return "NO ACTION";
        }
        switch (action) {
            case NO_ACTION:
                return "NO ACTION";
            case RESTRICT:
                return "RESTRICT";
            case CASCADE:
                return "CASCADE";
            case SET_NULL:
                return "SET NULL";
            case SET_DEFAULT:
                return "SET DEFAULT";
            default:
                return "NO ACTION";
        }
    }

    @Override
    public String buildIndex(JQuickIndexDefinition index) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE ");
        if (index.isUnique()) {
            sb.append("UNIQUE ");
        }
        if (index.getType() != null) {
            switch (index.getType()) {
                case "BTREE":
                    sb.append("INDEX ").append(quoteIdentifier(index.getIndexName()));
                    sb.append(" USING BTREE");
                    break;
                case "HASH":
                    sb.append("INDEX ").append(quoteIdentifier(index.getIndexName()));
                    sb.append(" USING HASH");
                    break;
                case "GIST":
                    sb.append("INDEX ").append(quoteIdentifier(index.getIndexName()));
                    sb.append(" USING GIST");
                    break;
                case "GIN":
                    sb.append("INDEX ").append(quoteIdentifier(index.getIndexName()));
                    sb.append(" USING GIN");
                    break;
                case "FULLTEXT":
                    sb.append("INDEX ").append(quoteIdentifier(index.getIndexName()));
                    sb.append(" USING GIN");
                    break;
                default:
                    sb.append("INDEX ").append(quoteIdentifier(index.getIndexName()));
                    break;
            }
        } else {
            sb.append("INDEX ").append(quoteIdentifier(index.getIndexName()));
        }

        sb.append(" ON ").append("${tableName}").append(" (");
        sb.append(formatColumnList(index.getColumns()));
        sb.append(")");
        if (index.getComment() != null && !index.getComment().isEmpty()) {
            sb.append(" WHERE ").append(index.getComment());
        }

        return sb.toString();
    }



    @Override
    public String buildCreateTable(JQuickTableDefinition table) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE ").append(quoteIdentifier(table.getTableName())).append(" (\n");
        appendColumnDefinitions(sql, table);
        appendPrimaryKey(sql, table);
        appendUniqueConstraints(sql, table);
        appendForeignKeys(sql, table);
        removeTrailingComma(sql);
        sql.append(")");
        appendTableOptions(sql, table);
        String tableComment = buildTableComment(table);
        if (tableComment != null && !tableComment.isEmpty()) {
            sql.append(";\n").append(tableComment);
        }
        return sql.toString();
    }

    /**
     * 构建表注释语句（KingbaseES 使用 COMMENT ON 语法）
     */
    protected String buildTableComment(JQuickTableDefinition table) {
        if (table.getComment() != null && !table.getComment().isEmpty()) {
            return "COMMENT ON TABLE " + quoteIdentifier(table.getTableName())
                    + " IS '" + escapeString(table.getComment()) + "'";
        }
        return null;
    }

    /**
     * 构建列注释语句
     */
    public String buildColumnComment(String tableName, JQuickColumnDefinition column) {
        if (column.getComment() != null && !column.getComment().isEmpty()) {
            return "COMMENT ON COLUMN " + quoteIdentifier(tableName) + "."
                    + quoteIdentifier(column.getColumnName())
                    + " IS '" + escapeString(column.getComment()) + "'";
        }
        return null;
    }

    /**
     * 构建所有列注释语句
     */
    public String[] buildAllColumnComments(String tableName, JQuickTableDefinition table) {
        return table.getColumns().stream()
                .map(col -> buildColumnComment(tableName, col))
                .filter(comment -> comment != null && !comment.isEmpty())
                .toArray(String[]::new);
    }

    /**
     * 构建完整建表语句（包含注释）
     */
    public String buildCreateTableWithComments(JQuickTableDefinition table) {
        StringBuilder fullSql = new StringBuilder();
        fullSql.append(buildCreateTable(table));
        for (JQuickColumnDefinition column : table.getColumns()) {
            String columnComment = buildColumnComment(table.getTableName(), column);
            if (columnComment != null && !columnComment.isEmpty()) {
                fullSql.append(";\n").append(columnComment);
            }
        }

        return fullSql.toString();
    }

    /**
     * 构建创建序列语句（用于自增列）
     */
    public String buildCreateSequence(String sequenceName) {
        return "CREATE SEQUENCE IF NOT EXISTS " + quoteIdentifier(sequenceName);
    }

    /**
     * 构建创建自增列序列语句
     */
    public String buildCreateSequenceForColumn(String tableName, String columnName) {
        String seqName = tableName + "_" + columnName + SEQ_SUFFIX;
        return buildCreateSequence(seqName);
    }

    /**
     * 构建设置序列起始值语句
     */
    public String buildSetSequenceStartValue(String sequenceName, long startValue) {
        return "ALTER SEQUENCE " + quoteIdentifier(sequenceName) + " START WITH " + startValue;
    }

    /**
     * 构建获取序列当前值语句
     */
    public String buildGetSequenceCurrentValue(String sequenceName) {
        return "SELECT CURRVAL('" + sequenceName + "')";
    }

    /**
     * 构建获取序列下一个值语句
     */
    public String buildGetSequenceNextValue(String sequenceName) {
        return "SELECT NEXTVAL('" + sequenceName + "')";
    }

    /**
     * 构建 ALTER COLUMN 设置默认值语句
     */
    public String buildSetDefaultValue(String tableName, String columnName, Object defaultValue) {
        return "ALTER TABLE " + quoteIdentifier(tableName)
                + " ALTER COLUMN " + quoteIdentifier(columnName)
                + " SET DEFAULT " + formatDefaultValue(defaultValue);
    }

    /**
     * 构建 ALTER COLUMN 删除默认值语句
     */
    public String buildDropDefaultValue(String tableName, String columnName) {
        return "ALTER TABLE " + quoteIdentifier(tableName) + " ALTER COLUMN " + quoteIdentifier(columnName) + " DROP DEFAULT";
    }

    /**
     * 构建 ALTER COLUMN 设置 NOT NULL 语句
     */
    public String buildSetNotNull(String tableName, String columnName) {
        return "ALTER TABLE " + quoteIdentifier(tableName) + " ALTER COLUMN " + quoteIdentifier(columnName) + " SET NOT NULL";
    }

    /**
     * 构建 ALTER COLUMN 删除 NOT NULL 语句
     */
    public String buildDropNotNull(String tableName, String columnName) {
        return "ALTER TABLE " + quoteIdentifier(tableName) + " ALTER COLUMN " + quoteIdentifier(columnName) + " DROP NOT NULL";
    }

    /**
     * 构建 RENAME COLUMN 语句
     */
    public String buildRenameColumn(String tableName, String oldName, String newName) {
        return "ALTER TABLE " + quoteIdentifier(tableName) + " RENAME COLUMN " + quoteIdentifier(oldName) + " TO " + quoteIdentifier(newName);
    }

    /**
     * 构建 ALTER COLUMN TYPE 语句
     */
    public String buildAlterColumnType(String tableName, String columnName, JQuickDataType newDataType) {
        return "ALTER TABLE " + quoteIdentifier(tableName) + " ALTER COLUMN " + quoteIdentifier(columnName) + " TYPE " + getDataTypeString(newDataType);
    }

    /**
     * 构建 DROP CONSTRAINT 语句
     */
    public String buildDropConstraint(String tableName, String constraintName) {
        return "ALTER TABLE " + quoteIdentifier(tableName) + " DROP CONSTRAINT " + quoteIdentifier(constraintName);
    }

    /**
     * 构建 VACUUM 语句（KingbaseES 优化）
     */
    public String buildVacuum(String tableName) {
        return "VACUUM " + quoteIdentifier(tableName);
    }

    /**
     * 构建 ANALYZE 语句（更新统计信息）
     */
    public String buildAnalyze(String tableName) {
        return "ANALYZE " + quoteIdentifier(tableName);
    }
}