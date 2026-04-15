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
import com.github.paohaijiao.row.JQuickRow;
import com.github.paohaijiao.table.JQuickTableDefinition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
    public String getAutoIncrementKeyword(JQuickTableDefinition tableDefinition) {
        return "SERIAL";
    }

    @Override
    public String buildModifyColumn(JQuickTableDefinition tableDefinition,String tableName, JQuickColumnDefinition column) {
        return "ALTER TABLE " + quoteIdentifier(tableDefinition,tableName) + " ALTER COLUMN " + quoteIdentifier(tableDefinition,column.getColumnName()) + " TYPE " + getDataTypeString(tableDefinition,column.getDataType());
    }

    @Override
    public String buildChangeColumn(JQuickTableDefinition tableDefinition,String tableName, String oldName, JQuickColumnDefinition newColumn) {
        StringBuilder sb = new StringBuilder();
        if (!oldName.equals(newColumn.getColumnName())) {
            sb.append("ALTER TABLE ").append(quoteIdentifier(tableDefinition,tableName)).append(" RENAME COLUMN ").append(quoteIdentifier(tableDefinition,oldName)).append(" TO ").append(quoteIdentifier(tableDefinition,newColumn.getColumnName())).append(";\n");
        }
        sb.append("ALTER TABLE ").append(quoteIdentifier(tableDefinition,tableName)).append(" ALTER COLUMN ").append(quoteIdentifier(tableDefinition,newColumn.getColumnName())).append(" TYPE ").append(getDataTypeString(tableDefinition,newColumn.getDataType()));
        return sb.toString();
    }

    @Override
    public String buildShowCreateTable(JQuickTableDefinition tableDefinition,String tableName) {
        return "SELECT pg_get_tabledef('" + quoteIdentifier(tableDefinition,tableName) + "')";
    }

    @Override
    public String buildDescribeTable(JQuickTableDefinition tableDefinition,String tableName) {
        return "SELECT column_name, data_type, is_nullable, column_default " + "FROM information_schema.columns " + "WHERE table_name = '" + tableName + "' " + "ORDER BY ordinal_position";
    }

    @Override
    public String buildInsert(JQuickTableDefinition table,JQuickRow row ) {
        if (row == null || row.isEmpty() || table == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(quoteIdentifier(table,table.getTableName())).append(" (");
        List<String> columns = new ArrayList<>(row.keySet());
        sb.append(columns.stream().map(e->quoteIdentifier(table,e)).collect(Collectors.joining(", ")));
        sb.append(") VALUES (");
        List<String> values = new ArrayList<>();
        for (String col : columns) {
            values.add(formatValue(row.get(col)));
        }
        sb.append(String.join(", ", values));
        sb.append(")");
        if (!table.getPrimaryKeys().isEmpty()) {
            String pkColumn = table.getPrimaryKeys().get(0).getColumns().get(0);
            sb.append(" RETURNING ").append(quoteIdentifier(table,pkColumn));
        }

        return sb.toString();
    }

    @Override
    public String buildUpdate(JQuickTableDefinition table,JQuickRow row,  String whereClause) {
        if (row == null || row.isEmpty() || table == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE ").append(quoteIdentifier(table,table.getTableName())).append(" SET ");
        List<String> setClauses = new ArrayList<>();
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            setClauses.add(quoteIdentifier(table,entry.getKey()) + " = " + formatValue(entry.getValue()));
        }
        sb.append(String.join(", ", setClauses));
        if (whereClause != null && !whereClause.trim().isEmpty()) {
            sb.append(" WHERE ").append(whereClause);
        }
        return sb.toString();
    }

    @Override
    public String buildDelete(JQuickTableDefinition table, String whereClause) {
        if (table == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("DELETE FROM ").append(quoteIdentifier(table,table.getTableName()));
        if (whereClause != null && !whereClause.trim().isEmpty()) {
            sb.append(" WHERE ").append(whereClause);
        }
        return sb.toString();
    }

    @Override
    public String buildSelect(JQuickTableDefinition table, List<String> columns, String whereClause) {
        if (table == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        if (columns == null || columns.isEmpty()) {
            sb.append("*");
        } else {
            sb.append(columns.stream().map(e->quoteIdentifier(table,e)).collect(Collectors.joining(", ")));
        }
        sb.append(" FROM ").append(quoteIdentifier(table,table.getTableName()));
        if (whereClause != null && !whereClause.trim().isEmpty()) {
            sb.append(" WHERE ").append(whereClause);
        }
        return sb.toString();
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
                sql.append(" TABLESPACE ").append(quoteIdentifier(table,tablespace));
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
    public String buildIndex(JQuickTableDefinition table, JQuickIndexDefinition index) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE ");
        if (index.isUnique()) {
            sb.append("UNIQUE ");
        }
        if (index.getType() != null) {
            switch (index.getType()) {
                case "BTREE":
                    sb.append("INDEX ").append(quoteIdentifier(table,index.getIndexName()));
                    sb.append(" USING BTREE");
                    break;
                case "HASH":
                    sb.append("INDEX ").append(quoteIdentifier(table,index.getIndexName()));
                    sb.append(" USING HASH");
                    break;
                case "GIST":
                    sb.append("INDEX ").append(quoteIdentifier(table,index.getIndexName()));
                    sb.append(" USING GIST");
                    break;
                case "GIN":
                    sb.append("INDEX ").append(quoteIdentifier(table,index.getIndexName()));
                    sb.append(" USING GIN");
                    break;
                case "FULLTEXT":
                    sb.append("INDEX ").append(quoteIdentifier(table,index.getIndexName()));
                    sb.append(" USING GIN");
                    break;
                default:
                    sb.append("INDEX ").append(quoteIdentifier(table,index.getIndexName()));
                    break;
            }
        } else {
            sb.append("INDEX ").append(quoteIdentifier(table,index.getIndexName()));
        }

        sb.append(" ON ").append("${tableName}").append(" (");
        sb.append(formatColumnList(table,index.getColumns()));
        sb.append(")");
        if (index.getComment() != null && !index.getComment().isEmpty()) {
            sb.append(" WHERE ").append(index.getComment());
        }

        return sb.toString();
    }



    @Override
    public String buildCreateTable(JQuickTableDefinition tableDefinition) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE ").append(quoteIdentifier(tableDefinition,tableDefinition.getTableName())).append(" (\n");
        appendColumnDefinitions(sql, tableDefinition);
        appendPrimaryKey(sql, tableDefinition);
        appendUniqueConstraints(sql, tableDefinition);
        appendForeignKeys(sql, tableDefinition);
        removeTrailingComma(sql);
        sql.append(")");
        appendTableOptions(sql, tableDefinition);
        String tableComment = buildTableComment(tableDefinition);
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
            return "COMMENT ON TABLE " + quoteIdentifier(table,table.getTableName()) + " IS '" + escapeString(table.getComment()) + "'";
        }
        return null;
    }

    /**
     * 构建列注释语句
     */
    public String buildColumnComment(JQuickTableDefinition table,String tableName, JQuickColumnDefinition column) {
        if (column.getComment() != null && !column.getComment().isEmpty()) {
            return "COMMENT ON COLUMN " + quoteIdentifier(table,tableName) + "."
                    + quoteIdentifier(table,column.getColumnName())
                    + " IS '" + escapeString(column.getComment()) + "'";
        }
        return null;
    }

    /**
     * 构建所有列注释语句
     */
    public String[] buildAllColumnComments(JQuickTableDefinition table,String tableName) {
        return table.getColumns().stream()
                .map(col -> buildColumnComment(table,tableName, col))
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
            String columnComment = buildColumnComment(table,table.getTableName(), column);
            if (columnComment != null && !columnComment.isEmpty()) {
                fullSql.append(";\n").append(columnComment);
            }
        }

        return fullSql.toString();
    }

    /**
     * 构建创建序列语句（用于自增列）
     */
    public String buildCreateSequence(JQuickTableDefinition table,String sequenceName) {
        return "CREATE SEQUENCE IF NOT EXISTS " + quoteIdentifier(table,sequenceName);
    }

    /**
     * 构建创建自增列序列语句
     */
    public String buildCreateSequenceForColumn(JQuickTableDefinition table,String tableName, String columnName) {
        String seqName = tableName + "_" + columnName + SEQ_SUFFIX;
        return buildCreateSequence(table,seqName);
    }

    /**
     * 构建设置序列起始值语句
     */
    public String buildSetSequenceStartValue(JQuickTableDefinition table,String sequenceName, long startValue) {
        return "ALTER SEQUENCE " + quoteIdentifier(table,sequenceName) + " START WITH " + startValue;
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
    public String buildSetDefaultValue(JQuickTableDefinition table,String tableName, String columnName, Object defaultValue) {
        return "ALTER TABLE " + quoteIdentifier(table,tableName)
                + " ALTER COLUMN " + quoteIdentifier(table,columnName)
                + " SET DEFAULT " + formatDefaultValue(defaultValue);
    }

    /**
     * 构建 ALTER COLUMN 删除默认值语句
     */
    public String buildDropDefaultValue(JQuickTableDefinition table,String tableName, String columnName) {
        return "ALTER TABLE " + quoteIdentifier(table,tableName) + " ALTER COLUMN " + quoteIdentifier(table,columnName) + " DROP DEFAULT";
    }

    /**
     * 构建 ALTER COLUMN 设置 NOT NULL 语句
     */
    public String buildSetNotNull(JQuickTableDefinition table,String tableName, String columnName) {
        return "ALTER TABLE " + quoteIdentifier(table,tableName) + " ALTER COLUMN " + quoteIdentifier(table,columnName) + " SET NOT NULL";
    }

    /**
     * 构建 ALTER COLUMN 删除 NOT NULL 语句
     */
    public String buildDropNotNull(JQuickTableDefinition table,String tableName, String columnName) {
        return "ALTER TABLE " + quoteIdentifier(table,tableName) + " ALTER COLUMN " + quoteIdentifier(table,columnName) + " DROP NOT NULL";
    }

    /**
     * 构建 RENAME COLUMN 语句
     */
    public String buildRenameColumn(JQuickTableDefinition table,String tableName, String oldName, String newName) {
        return "ALTER TABLE " + quoteIdentifier(table,tableName) + " RENAME COLUMN " + quoteIdentifier(table,oldName) + " TO " + quoteIdentifier(table,newName);
    }

    /**
     * 构建 ALTER COLUMN TYPE 语句
     */
    public String buildAlterColumnType(JQuickTableDefinition table,String tableName, String columnName, JQuickDataType newDataType) {
        return "ALTER TABLE " + quoteIdentifier(table,tableName) + " ALTER COLUMN " + quoteIdentifier(table,columnName) + " TYPE " + getDataTypeString(table,newDataType);
    }

    /**
     * 构建 DROP CONSTRAINT 语句
     */
    public String buildDropConstraint(JQuickTableDefinition table,String tableName, String constraintName) {
        return "ALTER TABLE " + quoteIdentifier(table,tableName) + " DROP CONSTRAINT " + quoteIdentifier(table,constraintName);
    }

    /**
     * 构建 VACUUM 语句（KingbaseES 优化）
     */
    public String buildVacuum(JQuickTableDefinition table,String tableName) {
        return "VACUUM " + quoteIdentifier(table,tableName);
    }

    /**
     * 构建 ANALYZE 语句（更新统计信息）
     */
    public String buildAnalyze(JQuickTableDefinition tableDefinition,String tableName) {
        return "ANALYZE " + quoteIdentifier(tableDefinition,tableName);
    }
}