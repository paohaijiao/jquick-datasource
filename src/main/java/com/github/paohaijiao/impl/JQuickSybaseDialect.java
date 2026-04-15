package com.github.paohaijiao.impl;
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

import com.github.paohaijiao.column.JQuickColumnDefinition;
import com.github.paohaijiao.dataType.JQuickDataType;
import com.github.paohaijiao.dataType.JQuickDataTypeConverter;
import com.github.paohaijiao.dataType.impl.JQuickSybaseDataTypeConverter;
import com.github.paohaijiao.dialect.JQuickAbsSQLDialect;
import com.github.paohaijiao.enums.JQuickForeignKeyAction;
import com.github.paohaijiao.extra.JQuickForeignKeyConstraint;
import com.github.paohaijiao.extra.JQuickIndexDefinition;
import com.github.paohaijiao.row.JQuickRow;
import com.github.paohaijiao.table.JQuickTableDefinition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Sybase 方言实现
 * 支持 Sybase Adaptive Server Enterprise (ASE) 数据库的 SQL 语法特性
 *
 * Sybase ASE 版本兼容性：
 * - ASE 15.7+
 * - ASE 16.0+
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/15
 */
public class JQuickSybaseDialect extends JQuickAbsSQLDialect {

    protected static final String SYBASE_QUOTE = "\"";

    private static final String SEQ_SUFFIX = "_SEQ";

    @Override
    protected JQuickDataTypeConverter createDataTypeConvert() {
        return new JQuickSybaseDataTypeConverter();
    }

    @Override
    protected String getQuoteKeyWord() {
        return SYBASE_QUOTE;
    }

    @Override
    public String getAutoIncrementKeyword(JQuickTableDefinition tableDefinition) {
        // Sybase 使用 IDENTITY 关键字
        return "IDENTITY";
    }

    @Override
    protected void appendTableOptions(StringBuilder sql, JQuickTableDefinition table) {
        // Sybase 表选项
        if (table.getExtensions() != null) {
            // 锁定模式
            if (table.getExtensions().containsKey("lockMode")) {
                String lockMode = table.getExtensions().get("lockMode").toString();
                if ("datarows".equalsIgnoreCase(lockMode)) {
                    sql.append(" LOCK DATAROWS");
                } else if ("datapages".equalsIgnoreCase(lockMode)) {
                    sql.append(" LOCK DATAPAGES");
                } else if ("allpages".equalsIgnoreCase(lockMode)) {
                    sql.append(" LOCK ALLPAGES");
                }
            }
            // 表空间（segment）
            if (table.getExtensions().containsKey("segment")) {
                String segment = table.getExtensions().get("segment").toString();
                sql.append(" ON ").append(quoteIdentifier(table, segment));
            }
            // 分区数
            if (table.getExtensions().containsKey("partitionCount")) {
                Integer partitionCount = (Integer) table.getExtensions().get("partitionCount");
                if (partitionCount != null && partitionCount > 0) {
                    sql.append(" PARTITION BY ROUNDROBIN ").append(partitionCount);
                }
            }
            // 填充因子
            if (table.getExtensions().containsKey("fillFactor")) {
                Integer fillFactor = (Integer) table.getExtensions().get("fillFactor");
                if (fillFactor != null && fillFactor > 0) {
                    sql.append(" WITH FILLFACTOR = ").append(fillFactor);
                }
            }
            // 最大行数
            if (table.getExtensions().containsKey("maxRowsPerPage")) {
                Integer maxRows = (Integer) table.getExtensions().get("maxRowsPerPage");
                if (maxRows != null && maxRows > 0) {
                    sql.append(" MAX_ROWS_PER_PAGE = ").append(maxRows);
                }
            }
            // 预留空间
            if (table.getExtensions().containsKey("reservePageGap")) {
                Integer reserveGap = (Integer) table.getExtensions().get("reservePageGap");
                if (reserveGap != null && reserveGap > 0) {
                    sql.append(" RESERVEPAGEGAP = ").append(reserveGap);
                }
            }
        }
    }

    @Override
    public String buildCreateTable(JQuickTableDefinition table) {
        StringBuilder sql = new StringBuilder();
        // 临时表
        boolean isTemporary = false;
        if (table.getExtensions() != null && table.getExtensions().containsKey("temporary")) {
            Boolean temporary = (Boolean) table.getExtensions().get("temporary");
            if (temporary != null && temporary) {
                isTemporary = true;
                // Sybase 临时表使用 # 前缀
                String tableName = table.getTableName();
                if (!tableName.startsWith("#")) {
                    table.setTableName("#" + tableName);
                }
                sql.append("CREATE TABLE ");
            } else {
                sql.append("CREATE TABLE ");
            }
        } else {
            sql.append("CREATE TABLE ");
        }
        sql.append(quoteIdentifier(table, table.getTableName())).append(" (\n");
        appendColumnDefinitions(sql, table);
        appendPrimaryKey(sql, table);
        appendUniqueConstraints(sql, table);
        appendForeignKeys(sql, table);
        appendCheckConstraints(sql, table);
        removeTrailingComma(sql);
        sql.append(")");
        appendTableOptions(sql, table);
        // 添加表注释
        String tableComment = buildTableComment(table);
        if (tableComment != null && !tableComment.isEmpty()) {
            sql.append(";\n").append(tableComment);
        }
        return sql.toString();
    }

    /**
     * 追加检查约束
     */
    protected void appendCheckConstraints(StringBuilder sql, JQuickTableDefinition table) {
        if (table.getExtensions() != null && table.getExtensions().containsKey("checkConstraints")) {
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> checks = (List<Map<String, Object>>) table.getExtensions().get("checkConstraints");
            if (checks != null) {
                for (Map<String, Object> check : checks) {
                    String constraintName = (String) check.get("name");
                    String condition = (String) check.get("condition");
                    if (condition != null && !condition.isEmpty()) {
                        sql.append(INDENT);
                        if (constraintName != null && !constraintName.isEmpty()) {
                            sql.append("CONSTRAINT ").append(quoteIdentifier(table, constraintName)).append(" ");
                        }
                        sql.append("CHECK (").append(condition).append(")");
                        sql.append(COMMA).append(NEW_LINE);
                    }
                }
            }
        }
    }

    /**
     * 构建表注释语句
     */
    protected String buildTableComment(JQuickTableDefinition table) {
        if (table.getComment() != null && !table.getComment().isEmpty()) {
            // Sybase 使用 COMMENT ON TABLE 语句
            return "COMMENT ON TABLE " + quoteIdentifier(table, table.getTableName()) + " IS '" + escapeString(table.getComment()) + "'";
        }
        return null;
    }

    /**
     * 构建列注释语句
     */
    public String buildColumnComment(JQuickTableDefinition table, String tableName, JQuickColumnDefinition column) {
        if (column.getComment() != null && !column.getComment().isEmpty()) {
            return "COMMENT ON COLUMN " + quoteIdentifier(table, tableName) + "." + quoteIdentifier(table, column.getColumnName()) + " IS '" + escapeString(column.getComment()) + "'";
        }
        return null;
    }

    /**
     * 构建所有列注释语句
     */
    public String[] buildAllColumnComments(JQuickTableDefinition table, String tableName) {
        return table.getColumns().stream()
                .map(col -> buildColumnComment(table, tableName, col))
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
            String columnComment = buildColumnComment(table, table.getTableName(), column);
            if (columnComment != null && !columnComment.isEmpty()) {
                fullSql.append(";\n").append(columnComment);
            }
        }
        return fullSql.toString();
    }

    @Override
    public String buildColumnDefinition(JQuickTableDefinition table, JQuickColumnDefinition column) {
        StringBuilder def = new StringBuilder();
        def.append(quoteIdentifier(table, column.getColumnName())).append(SPACE);
        // 数据类型
        if (column.isAutoIncrement()) {
            def.append(getAutoIncrementKeyword(table));
            // Sybase IDENTITY 列必须是 numeric 或 decimal
            def.append("(1,0)");
        } else {
            def.append(getDataTypeString(table, column.getDataType()));
        }
        appendNullClause(def, column);
        appendDefaultClause(def, column);
        return def.toString();
    }

    @Override
    protected void appendNullClause(StringBuilder def, JQuickColumnDefinition column) {
        if (!column.isNullable()) {
            def.append(" NOT NULL");
        }
    }

    @Override
    protected void appendDefaultClause(StringBuilder def, JQuickColumnDefinition column) {
        if (column.getDefaultValue() != null && !column.isAutoIncrement()) {
            def.append(" DEFAULT ").append(formatDefaultValue(column.getDefaultValue()));
        }
    }

    @Override
    protected void appendColumnComment(StringBuilder def, JQuickColumnDefinition column) {
        // Sybase 使用 COMMENT ON 语句，不在列定义中
    }

    @Override
    protected String getSpecialDefaultKeyword(String value) {
        if (value == null) {
            return null;
        }
        String upperValue = value.toUpperCase();
        if ("GETDATE()".equals(upperValue) || "CURRENT_TIMESTAMP".equals(upperValue) || "NOW()".equals(upperValue)) {
            return "GETDATE()";
        }
        if ("CURRENT_DATE".equals(upperValue)) {
            return "CONVERT(DATE, GETDATE())";
        }
        if ("CURRENT_TIME".equals(upperValue)) {
            return "CONVERT(TIME, GETDATE())";
        }
        if ("USER".equals(upperValue) || "CURRENT_USER".equals(upperValue)) {
            return "USER_NAME()";
        }
        if ("NEWID".equals(upperValue)) {
            return "NEWID()";
        }
        return null;
    }

    @Override
    protected String formatBooleanDefault(boolean value) {
        return value ? "1" : "0";
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
                return "NO ACTION";
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
    public String buildModifyColumn(JQuickTableDefinition table, String tableName, JQuickColumnDefinition column) {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ").append(quoteIdentifier(table, tableName));
        sb.append(" MODIFY ").append(quoteIdentifier(table, column.getColumnName()));
        sb.append(" ").append(getDataTypeString(table, column.getDataType()));
        if (!column.isNullable()) {
            sb.append(" NOT NULL");
        }
        if (column.getDefaultValue() != null && !column.isAutoIncrement()) {
            sb.append(" DEFAULT ").append(formatDefaultValue(column.getDefaultValue()));
        }
        return sb.toString();
    }

    @Override
    public String buildChangeColumn(JQuickTableDefinition table, String tableName, String oldName, JQuickColumnDefinition newColumn) {
        StringBuilder sb = new StringBuilder();

        if (!oldName.equals(newColumn.getColumnName())) {
            // Sybase 重命名列
            sb.append("EXEC sp_rename '")
                    .append(quoteIdentifier(table, tableName)).append(".")
                    .append(quoteIdentifier(table, oldName)).append("', '")
                    .append(newColumn.getColumnName()).append("', 'COLUMN';\n");
        }
        sb.append("ALTER TABLE ").append(quoteIdentifier(table, tableName));
        sb.append(" MODIFY ").append(quoteIdentifier(table, newColumn.getColumnName()));
        sb.append(" ").append(getDataTypeString(table, newColumn.getDataType()));
        if (!newColumn.isNullable()) {
            sb.append(" NOT NULL");
        }
        if (newColumn.getDefaultValue() != null && !newColumn.isAutoIncrement()) {
            sb.append(" DEFAULT ").append(formatDefaultValue(newColumn.getDefaultValue()));
        }
        return sb.toString();
    }

    @Override
    public String buildShowCreateTable(JQuickTableDefinition table, String tableName) {
        // Sybase 使用 sp_help 或查询系统表
        return "sp_help " + quoteIdentifier(table, tableName);
    }

    @Override
    public String buildDescribeTable(JQuickTableDefinition table, String tableName) {
        return "SELECT " +
                "  c.name AS column_name, " +
                "  t.name AS data_type, " +
                "  c.status & 8 AS is_nullable, " +
                "  c.default_text AS column_default " +
                "FROM syscolumns c " +
                "INNER JOIN systypes t ON c.type = t.type " +
                "WHERE c.id = OBJECT_ID('" + tableName + "') " +
                "ORDER BY c.colid";
    }

    @Override
    public String buildInsert(JQuickTableDefinition table, JQuickRow row) {
        if (row == null || row.isEmpty() || table == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(quoteIdentifier(table, table.getTableName())).append(" (");
        List<String> columns = new ArrayList<>(row.keySet());
        sb.append(columns.stream().map(e -> quoteIdentifier(table, e)).collect(Collectors.joining(", ")));
        sb.append(") VALUES (");
        List<String> values = new ArrayList<>();
        for (String col : columns) {
            values.add(formatValue(row.get(col)));
        }
        sb.append(String.join(", ", values));
        sb.append(")");
        // Sybase 使用 SELECT @@IDENTITY 获取自增列值
        if (!table.getPrimaryKeys().isEmpty() && hasAutoIncrement(table)) {
            sb.append(";\nSELECT @@IDENTITY");
        }

        return sb.toString();
    }

    @Override
    public String buildUpdate(JQuickTableDefinition table, JQuickRow row, String whereClause) {
        if (row == null || row.isEmpty() || table == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE ").append(quoteIdentifier(table, table.getTableName())).append(" SET ");
        List<String> setClauses = new ArrayList<>();
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            setClauses.add(quoteIdentifier(table, entry.getKey()) + " = " + formatValue(entry.getValue()));
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
        sb.append("DELETE FROM ").append(quoteIdentifier(table, table.getTableName()));
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
        // TOP 子句
        if (table.getExtensions() != null && table.getExtensions().containsKey("top")) {
            Integer top = (Integer) table.getExtensions().get("top");
            if (top != null && top > 0) {
                sb.append("TOP ").append(top).append(" ");
            }
        }
        if (columns == null || columns.isEmpty()) {
            sb.append("*");
        } else {
            sb.append(columns.stream().map(e -> quoteIdentifier(table, e)).collect(Collectors.joining(", ")));
        }
        sb.append(" FROM ").append(quoteIdentifier(table, table.getTableName()));
        if (whereClause != null && !whereClause.trim().isEmpty()) {
            sb.append(" WHERE ").append(whereClause);
        }
        return sb.toString();
    }

    @Override
    public String buildIndex(JQuickTableDefinition table, JQuickIndexDefinition index) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE ");
        if (index.isUnique()) {
            sb.append("UNIQUE ");
        }
        if (index.getType() != null && "CLUSTERED".equalsIgnoreCase(index.getType())) {
            sb.append("CLUSTERED ");
        } else if (index.getType() != null && "NON_CLUSTERED".equalsIgnoreCase(index.getType())) {
            sb.append("NONCLUSTERED ");
        }
        sb.append("INDEX ").append(quoteIdentifier(table, index.getIndexName()));
        sb.append(" ON ").append("${tableName}").append(" (");
        sb.append(formatColumnList(table, index.getColumns()));
        sb.append(")");
        if (index.hasExtension("fillFactor")) {
            Integer fillFactor = index.getExtension("fillFactor");
            if (fillFactor != null && fillFactor > 0) {
                sb.append(" WITH FILLFACTOR = ").append(fillFactor);
            }
        }
        if (index.hasExtension("order")) {
            @SuppressWarnings("unchecked")
            Map<String, String> orderMap = index.getExtension("order");
            if (orderMap != null && !orderMap.isEmpty()) {
                sb.append(" ").append(orderMap.values().stream().findFirst().orElse(""));
            }
        }
        if (index.hasExtension("ignoreDupKey")) {
            Boolean ignoreDup = index.getExtension("ignoreDupKey");
            if (ignoreDup != null && ignoreDup) {
                sb.append(" WITH IGNORE_DUP_KEY");
            }
        }
        String segment = index.getFileGroup();
        if (segment != null && !segment.isEmpty()) {
            sb.append(" ON ").append(quoteIdentifier(table, segment));
        }

        return sb.toString();
    }

    /**
     * 检查是否有自增列
     */
    private boolean hasAutoIncrement(JQuickTableDefinition table) {
        return table.getColumns().stream().anyMatch(JQuickColumnDefinition::isAutoIncrement);
    }

    /**
     * 构建创建序列语句
     */
    public String buildCreateSequence(JQuickTableDefinition table, String sequenceName) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE SEQUENCE ").append(quoteIdentifier(table, sequenceName));

        Map<String, Object> seqParams = null;
        if (table.getExtensions() != null && table.getExtensions().containsKey("sequenceParams")) {
            @SuppressWarnings("unchecked")
            Map<String, Object> params = (Map<String, Object>) table.getExtensions().get("sequenceParams");
            seqParams = params;
        }

        if (seqParams != null) {
            if (seqParams.containsKey("startWith")) {
                sb.append(" START WITH ").append(seqParams.get("startWith"));
            }
            if (seqParams.containsKey("incrementBy")) {
                sb.append(" INCREMENT BY ").append(seqParams.get("incrementBy"));
            }
            if (seqParams.containsKey("minValue")) {
                sb.append(" MINVALUE ").append(seqParams.get("minValue"));
            }
            if (seqParams.containsKey("maxValue")) {
                sb.append(" MAXVALUE ").append(seqParams.get("maxValue"));
            }
            if (seqParams.containsKey("cache")) {
                sb.append(" CACHE ").append(seqParams.get("cache"));
            }
            if (seqParams.containsKey("cycle")) {
                Boolean cycle = (Boolean) seqParams.get("cycle");
                if (cycle != null && cycle) {
                    sb.append(" CYCLE");
                } else {
                    sb.append(" NO CYCLE");
                }
            }
        }

        return sb.toString();
    }

    /**
     * 构建删除序列语句
     */
    public String buildDropSequence(JQuickTableDefinition table, String sequenceName) {
        return "DROP SEQUENCE " + quoteIdentifier(table, sequenceName);
    }

    /**
     * 构建获取序列下一个值语句
     */
    public String buildGetSequenceNextValue(String sequenceName) {
        return "SELECT " + sequenceName + ".NEXTVAL";
    }

    /**
     * 构建获取序列当前值语句
     */
    public String buildGetSequenceCurrentValue(String sequenceName) {
        return "SELECT " + sequenceName + ".CURRVAL";
    }
    /**
     * 构建添加列语句
     */
    @Override
    public String buildAddColumn(JQuickTableDefinition table, String tableName, JQuickColumnDefinition column) {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ").append(quoteIdentifier(table, tableName));
        sb.append(" ADD ").append(quoteIdentifier(table, column.getColumnName()));
        sb.append(" ").append(getDataTypeString(table, column.getDataType()));
        if (column.isAutoIncrement()) {
            sb.append(" ").append(getAutoIncrementKeyword(table));
        }
        if (!column.isNullable()) {
            sb.append(" NOT NULL");
        }
        if (column.getDefaultValue() != null && !column.isAutoIncrement()) {
            sb.append(" DEFAULT ").append(formatDefaultValue(column.getDefaultValue()));
        }
        return sb.toString();
    }

    /**
     * 构建删除列语句
     */
    @Override
    public String buildDropColumn(JQuickTableDefinition table, String tableName, String columnName) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " DROP " + quoteIdentifier(table, columnName);
    }

    /**
     * 构建重命名列语句
     */
    public String buildRenameColumn(JQuickTableDefinition table, String tableName, String oldName, String newName) {
        return "EXEC sp_rename '" + quoteIdentifier(table, tableName) + "." + quoteIdentifier(table, oldName) + "', '" + newName + "', 'COLUMN'";
    }

    /**
     * 构建设置 NOT NULL 语句
     */
    public String buildSetNotNull(JQuickTableDefinition table, String tableName, String columnName) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " MODIFY " + quoteIdentifier(table, columnName) + " NOT NULL";
    }

    /**
     * 构建删除 NOT NULL 语句
     */
    public String buildDropNotNull(JQuickTableDefinition table, String tableName, String columnName) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " MODIFY " + quoteIdentifier(table, columnName) + " NULL";
    }

    /**
     * 构建设置默认值语句
     */
    public String buildSetDefaultValue(JQuickTableDefinition table, String tableName, String columnName, Object defaultValue) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " MODIFY " + quoteIdentifier(table, columnName) + " DEFAULT " + formatDefaultValue(defaultValue);
    }

    /**
     * 构建删除默认值语句
     */
    public String buildDropDefaultValue(JQuickTableDefinition table, String tableName, String columnName) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " MODIFY " + quoteIdentifier(table, columnName) + " DEFAULT NULL";
    }
    /**
     * 构建删除表语句
     */
    @Override
    public String buildDropTable(JQuickTableDefinition table, String tableName, boolean ifExists) {
        StringBuilder sb = new StringBuilder();
        if (ifExists) {
            sb.append("IF OBJECT_ID('").append(tableName).append("') IS NOT NULL\n");
            sb.append("  DROP TABLE ").append(quoteIdentifier(table, tableName));
        } else {
            sb.append("DROP TABLE ").append(quoteIdentifier(table, tableName));
        }
        return sb.toString();
    }

    /**
     * 构建清空表语句
     */
    @Override
    public String buildTruncateTable(JQuickTableDefinition table, String tableName) {
        return "TRUNCATE TABLE " + quoteIdentifier(table, tableName);
    }

    /**
     * 构建重命名表语句
     */
    @Override
    public String buildRenameTable(JQuickTableDefinition table, String oldName, String newName) {
        return "EXEC sp_rename '" + quoteIdentifier(table, oldName) + "', '" + newName + "'";
    }
    /**
     * 构建添加主键语句
     */
    public String buildAddPrimaryKey(JQuickTableDefinition table, String tableName, String constraintName, List<String> columns) {
        StringBuilder sb = new StringBuilder("ALTER TABLE ");
        sb.append(quoteIdentifier(table, tableName));
        sb.append(" ADD CONSTRAINT ");
        if (constraintName != null && !constraintName.isEmpty()) {
            sb.append(quoteIdentifier(table, constraintName)).append(" ");
        }
        sb.append("PRIMARY KEY ");
        // 聚集索引选项
        if (table.getExtensions() != null && table.getExtensions().containsKey("clustered")) {
            Boolean clustered = (Boolean) table.getExtensions().get("clustered");
            if (clustered != null && clustered) {
                sb.append("CLUSTERED ");
            } else {
                sb.append("NONCLUSTERED ");
            }
        }
        sb.append("(").append(formatColumnList(table, columns)).append(")");
        return sb.toString();
    }

    /**
     * 构建添加唯一约束语句
     */
    public String buildAddUniqueConstraint(JQuickTableDefinition table, String tableName, String constraintName, List<String> columns) {
        StringBuilder sb = new StringBuilder("ALTER TABLE ");
        sb.append(quoteIdentifier(table, tableName));
        sb.append(" ADD CONSTRAINT ");
        if (constraintName != null && !constraintName.isEmpty()) {
            sb.append(quoteIdentifier(table, constraintName)).append(" ");
        }
        sb.append("UNIQUE (").append(formatColumnList(table, columns)).append(")");
        return sb.toString();
    }

    /**
     * 构建添加外键语句
     */
    public String buildAddForeignKey(JQuickTableDefinition table, String tableName, JQuickForeignKeyConstraint fk) {
        StringBuilder sb = new StringBuilder("ALTER TABLE ");
        sb.append(quoteIdentifier(table, tableName));
        sb.append(" ADD CONSTRAINT ");
        if (fk.getConstraintName() != null && !fk.getConstraintName().isEmpty()) {
            sb.append(quoteIdentifier(table, fk.getConstraintName())).append(" ");
        }
        sb.append("FOREIGN KEY (").append(formatColumnList(table, fk.getColumns()));
        sb.append(") REFERENCES ").append(quoteIdentifier(table, fk.getReferencedTable()));
        sb.append(" (").append(formatColumnList(table, fk.getReferencedColumns())).append(")");

        if (fk.getOnDelete() != null) {
            sb.append(" ON DELETE ").append(convertForeignKeyAction(fk.getOnDelete()));
        }
        if (fk.getOnUpdate() != null) {
            sb.append(" ON UPDATE ").append(convertForeignKeyAction(fk.getOnUpdate()));
        }

        return sb.toString();
    }

    /**
     * 构建删除约束语句
     */
    public String buildDropConstraint(JQuickTableDefinition table, String tableName, String constraintName) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " DROP CONSTRAINT " + quoteIdentifier(table, constraintName);
    }

    /**
     * 构建启用约束语句
     */
    public String buildEnableConstraint(JQuickTableDefinition table, String tableName, String constraintName) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " CHECK CONSTRAINT " + quoteIdentifier(table, constraintName);
    }

    /**
     * 构建禁用约束语句
     */
    public String buildDisableConstraint(JQuickTableDefinition table, String tableName, String constraintName) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " NOCHECK CONSTRAINT " + quoteIdentifier(table, constraintName);
    }
    /**
     * 构建分页查询语句（使用 TOP 和子查询）
     */
    public String buildPaginationQuery(JQuickTableDefinition table, List<String> columns, String whereClause,
                                       String orderBy, int offset, int limit) {
        if (offset == 0) {
            // 简单 TOP 查询
            if (table.getExtensions() == null) {
                table.setExtensions(new java.util.HashMap<>());
            }
            table.getExtensions().put("top", limit);
            String sql = buildSelect(table, columns, whereClause);
            if (orderBy != null && !orderBy.isEmpty()) {
                sql += " ORDER BY " + orderBy;
            }
            return sql;
        } else {
            // Sybase 使用 ROW_NUMBER() 分页
            StringBuilder sql = new StringBuilder();
            sql.append("SELECT * FROM (\n");
            sql.append("  SELECT ROW_NUMBER() OVER (").append(orderBy != null ? "ORDER BY " + orderBy : "").append(") AS row_num,\n");
            sql.append("    ").append(columns != null && !columns.isEmpty() ?
                    columns.stream().map(c -> quoteIdentifier(table, c)).collect(Collectors.joining(", ")) : "*");
            sql.append("\n  FROM ").append(quoteIdentifier(table, table.getTableName()));
            if (whereClause != null && !whereClause.isEmpty()) {
                sql.append("\n  WHERE ").append(whereClause);
            }
            sql.append("\n) AS t\n");
            sql.append("WHERE row_num > ").append(offset);
            if (limit > 0) {
                sql.append(" AND row_num <= ").append(offset + limit);
            }
            return sql.toString();
        }
    }

    /**
     * 构建 TOP 查询语句
     */
    public String buildTopQuery(JQuickTableDefinition table, List<String> columns, String whereClause, String orderBy, int top) {
        if (table.getExtensions() == null) {
            table.setExtensions(new java.util.HashMap<>());
        }
        table.getExtensions().put("top", top);
        String sql = buildSelect(table, columns, whereClause);
        if (orderBy != null && !orderBy.isEmpty()) {
            sql += " ORDER BY " + orderBy;
        }
        return sql;
    }
    /**
     * 构建更新统计信息语句
     */
    public String buildUpdateStatistics(JQuickTableDefinition table, String tableName) {
        return "UPDATE STATISTICS " + quoteIdentifier(table, tableName);
    }

    /**
     * 构建更新统计信息语句（指定索引）
     */
    public String buildUpdateStatistics(JQuickTableDefinition table, String tableName, String indexName) {
        return "UPDATE STATISTICS " + quoteIdentifier(table, tableName) + " " + quoteIdentifier(table, indexName);
    }

    /**
     * 构建重建索引语句
     */
    public String buildRebuildIndex(JQuickTableDefinition table, String tableName, String indexName) {
        if (indexName != null && !indexName.isEmpty()) {
            return "REBUILD INDEX " + quoteIdentifier(table, indexName) + " ON " + quoteIdentifier(table, tableName);
        }
        return "REBUILD INDEX ALL ON " + quoteIdentifier(table, tableName);
    }

    /**
     * 构建数据库检查点语句
     */
    public String buildCheckpoint() {
        return "CHECKPOINT";
    }
    /**
     * 构建查询当前数据库语句
     */
    public String buildCurrentDatabase() {
        return "SELECT DB_NAME()";
    }

    /**
     * 构建查询当前用户语句
     */
    public String buildCurrentUser() {
        return "SELECT USER_NAME()";
    }

    /**
     * 构建查询版本语句
     */
    public String buildVersion() {
        return "SELECT @@VERSION";
    }

    /**
     * 构建查询所有表语句
     */
    public String buildShowTables() {
        return "SELECT name FROM sysobjects WHERE type = 'U'";
    }
    /**
     * 构建设置隔离级别语句
     */
    public String buildSetIsolationLevel(int level) {
        if (level >= 0 && level <= 3) {
            return "SET TRANSACTION ISOLATION LEVEL " + level;
        }
        return "SET TRANSACTION ISOLATION LEVEL 1";
    }

    /**
     * 构建查询锁信息语句
     */
    public String buildShowLocks() {
        return "sp_lock";
    }
}