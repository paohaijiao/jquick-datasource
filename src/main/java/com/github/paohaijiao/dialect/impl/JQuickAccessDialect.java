package com.github.paohaijiao.dialect.impl;

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
import com.github.paohaijiao.connector.JQuickDataSourceConnector;
import com.github.paohaijiao.dataType.JQuickDataType;
import com.github.paohaijiao.dataType.JQuickDataTypeConverter;
import com.github.paohaijiao.dataType.impl.JQuickAccessDataTypeConverter;
import com.github.paohaijiao.dialect.JQuickAbsSQLDialect;
import com.github.paohaijiao.enums.JQuickForeignKeyAction;
import com.github.paohaijiao.extra.JQuickForeignKeyConstraint;
import com.github.paohaijiao.extra.JQuickIndexDefinition;
import com.github.paohaijiao.statement.JQuickRow;
import com.github.paohaijiao.table.JQuickTableDefinition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Microsoft Access 方言实现
 * 支持 Access 数据库的 SQL 语法特性
 *
 * 注意：Access SQL 功能有限，主要限制包括：
 * - 不支持 ALTER TABLE 的许多操作（如 DROP COLUMN、MODIFY COLUMN）
 * - 不支持序列/自增列的精细控制
 * - 不支持注释语句
 * - 外键约束支持有限
 * - 建议使用 DAO 或 ADO 进行表结构修改
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/15
 */
public class JQuickAccessDialect extends JQuickAbsSQLDialect {

    protected static final String ACCESS_QUOTE = "[";

    protected static final String ACCESS_QUOTE_END = "]";

    @Override
    protected JQuickDataTypeConverter createDataTypeConvert() {
        return new JQuickAccessDataTypeConverter();
    }

    @Override
    protected String getQuoteKeyWord() {
        return ACCESS_QUOTE;
    }

    /**
     * Access 使用方括号引用标识符
     */
    @Override
    protected String quoteIdentifier(JQuickTableDefinition tableDefinition, String identifier) {
        if (identifier == null || identifier.isEmpty()) {
            return identifier;
        }
        if ((identifier.startsWith(ACCESS_QUOTE) && identifier.endsWith(ACCESS_QUOTE_END)) ||
                (identifier.startsWith("\"") && identifier.endsWith("\""))) {
            return identifier;
        }
        if (tableDefinition != null && tableDefinition.isQuoteEnabled()) {
            return ACCESS_QUOTE + identifier + ACCESS_QUOTE_END;
        }
        // Access 中如果标识符包含空格或特殊字符，需要加方括号
        if (needsQuoting(identifier)) {
            return ACCESS_QUOTE + identifier + ACCESS_QUOTE_END;
        }
        return identifier;
    }

    /**
     * 检查标识符是否需要引用
     */
    private boolean needsQuoting(String identifier) {
        if (identifier == null || identifier.isEmpty()) {
            return false;
        }
        // 检查是否包含空格、特殊字符或者是保留字
        if (identifier.contains(" ") || identifier.contains("-") || identifier.contains(".")) {
            return true;
        }
        // 检查是否为 Access 保留字（简化版）
        String upper = identifier.toUpperCase();
        String[] keywords = {"SELECT", "INSERT", "UPDATE", "DELETE", "FROM", "WHERE",
                "ORDER", "BY", "GROUP", "HAVING", "TABLE", "INDEX", "CONSTRAINT",
                "PRIMARY", "KEY", "FOREIGN", "REFERENCES", "UNIQUE", "CHECK"};
        for (String keyword : keywords) {
            if (keyword.equals(upper)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public String getAutoIncrementKeyword(JQuickTableDefinition tableDefinition) {
        // Access 使用 AUTOINCREMENT
        return "AUTOINCREMENT";
    }

    @Override
    protected void appendTableOptions(StringBuilder sql, JQuickTableDefinition table) {
        // Access 表选项有限
        if (table.getExtensions() != null) {
            // 压缩选项
            if (table.getExtensions().containsKey("compress")) {
                Boolean compress = (Boolean) table.getExtensions().get("compress");
                if (compress != null && compress) {
                    // Access 中没有直接的压缩语法，仅在创建数据库时设置
                }
            }
        }
    }

    @Override
    public String getDriverClass(JQuickDataSourceConnector connector) {
        if(null!=connector){
            return connector.getDriverClass();
        }
        return "net.ucanaccess.jdbc.UcanaccessDriver";
    }

    @Override
    public String getUrl(JQuickDataSourceConnector connector) {
        if (connector == null) {
            throw new IllegalArgumentException("Connector cannot be null");
        }
        if (connector.getUrl() != null && !connector.getUrl().trim().isEmpty()) {
            return connector.getUrl();
        }
        String filePath = connector.getSchema();
        String password = connector.getPassword();
        if (filePath == null || filePath.trim().isEmpty()) {
            throw new IllegalStateException("Database file path (schema) is required for Access connection");
        }
        StringBuilder url = new StringBuilder();
        url.append("jdbc:ucanaccess://");
        String normalizedPath = filePath.trim();
        if (normalizedPath.contains("\\")) {
            normalizedPath = normalizedPath.replace("\\", "/");
        }
        url.append(normalizedPath);
        if (password != null && !password.trim().isEmpty()) {
            url.append(";password=").append(password);
        }
        return url.toString();
    }

    @Override
    public String buildCreateTable(JQuickTableDefinition table) {
        StringBuilder sql = new StringBuilder();
        // 临时表（Access 使用 LOCAL TEMPORARY TABLE）
        boolean isTemporary = false;
        if (table.getExtensions() != null && table.getExtensions().containsKey("temporary")) {
            Boolean temporary = (Boolean) table.getExtensions().get("temporary");
            if (temporary != null && temporary) {
                isTemporary = true;
                sql.append("CREATE LOCAL TEMPORARY TABLE ");
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
        // Access 不支持表注释
        if (table.getComment() != null && !table.getComment().isEmpty()) {
            // 添加 SQL 注释作为替代
            sql.insert(0, "-- " + escapeString(table.getComment()) + "\n");
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

    @Override
    public String buildColumnDefinition(JQuickTableDefinition table, JQuickColumnDefinition column) {
        StringBuilder def = new StringBuilder();
        def.append(quoteIdentifier(table, column.getColumnName())).append(SPACE);
        // 自增列处理
        if (column.isAutoIncrement()) {
            def.append(getAutoIncrementKeyword(table));
            // 指定起始值和步长
            if (column.getExtensions() != null) {
                Long seed = null;
                Long increment = null;
                if (column.getExtensions().containsKey("identitySeed")) {
                    seed = ((Number) column.getExtensions().get("identitySeed")).longValue();
                }
                if (column.getExtensions().containsKey("identityIncrement")) {
                    increment = ((Number) column.getExtensions().get("identityIncrement")).longValue();
                }
                if (seed != null && increment != null) {
                    def.append("(").append(seed).append(", ").append(increment).append(")");
                } else if (seed != null) {
                    def.append("(").append(seed).append(")");
                }
            }
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
        // Access 不支持列注释
        if (column.getComment() != null && !column.getComment().isEmpty()) {
            // 添加 SQL 注释作为替代
            def.append(" /* ").append(escapeString(column.getComment())).append(" */");
        }
    }

    @Override
    protected String getSpecialDefaultKeyword(String value) {
        if (value == null) {
            return null;
        }
        String upperValue = value.toUpperCase();
        if ("NOW()".equals(upperValue) || "CURRENT_TIMESTAMP".equals(upperValue)) {
            return "Now()";
        }
        if ("DATE()".equals(upperValue) || "CURRENT_DATE".equals(upperValue)) {
            return "Date()";
        }
        if ("TIME()".equals(upperValue) || "CURRENT_TIME".equals(upperValue)) {
            return "Time()";
        }
        return null;
    }

    @Override
    protected String formatBooleanDefault(boolean value) {
        return value ? "True" : "False";
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
        // Access 不支持 MODIFY COLUMN，需要使用 ALTER TABLE ALTER COLUMN
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ").append(quoteIdentifier(table, tableName));
        sb.append(" ALTER COLUMN ").append(quoteIdentifier(table, column.getColumnName()));
        sb.append(" ").append(getDataTypeString(table, column.getDataType()));
        if (!column.isNullable()) {
            sb.append(" NOT NULL");
        }
        return sb.toString();
    }

    @Override
    public String buildChangeColumn(JQuickTableDefinition table, String tableName, String oldName, JQuickColumnDefinition newColumn) {
        // Access 不支持直接重命名列，需要重建表
        // 这里返回重建表的提示
        StringBuilder sb = new StringBuilder();
        sb.append("-- Access does not support RENAME COLUMN directly.\n");
        sb.append("-- Please use the following steps:\n");
        sb.append("-- 1. Add new column: ").append(buildAddColumn(table, tableName, newColumn)).append("\n");
        sb.append("-- 2. Copy data: UPDATE ").append(quoteIdentifier(table, tableName))
                .append(" SET ").append(quoteIdentifier(table, newColumn.getColumnName()))
                .append(" = ").append(quoteIdentifier(table, oldName)).append("\n");
        sb.append("-- 3. Drop old column: ").append(buildDropColumn(table, tableName, oldName));
        return sb.toString();
    }

    @Override
    public String buildShowCreateTable(JQuickTableDefinition table, String tableName) {
        // Access 没有直接的 SHOW CREATE TABLE，使用 MSysObjects 查询
        return "SELECT * FROM MSysObjects WHERE Name = '" + tableName + "'";
    }

    @Override
    public String buildDescribeTable(JQuickTableDefinition table, String tableName) {
        return "SELECT * FROM MSysColumns WHERE Name = '" + tableName + "'";
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
        // DISTINCT
        if (table.getExtensions() != null && table.getExtensions().containsKey("distinct")) {
            Boolean distinct = (Boolean) table.getExtensions().get("distinct");
            if (distinct != null && distinct) {
                sb.append("DISTINCT ");
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
        sb.append("INDEX ").append(quoteIdentifier(table, index.getIndexName()));
        sb.append(" ON ").append("${tableName}").append(" (");
        sb.append(formatColumnList(table, index.getColumns()));
        sb.append(")");
        // Access 索引选项
        if (index.hasExtension("ignoreNulls")) {
            Boolean ignoreNulls = index.getExtension("ignoreNulls");
            if (ignoreNulls != null && ignoreNulls) {
                sb.append(" WITH IGNORE NULL");
            }
        }
        // 主键索引不允许重复
        if (index.isUnique() && index.getType() != null && "PRIMARY".equalsIgnoreCase(index.getType())) {
            sb.append(" PRIMARY");
        }
        // 聚集索引
        if (index.getType() != null && "CLUSTERED".equalsIgnoreCase(index.getType())) {
            sb.append(" WITH CLUSTER");
        }
        // 外键索引
        if (index.getType() != null && "FOREIGN".equalsIgnoreCase(index.getType())) {
            sb.append(" FOREIGN");
        }
        // 禁用重复
        if (index.isUnique() && index.hasExtension("disallowNull")) {
            Boolean disallowNull = index.getExtension("disallowNull");
            if (disallowNull != null && disallowNull) {
                sb.append(" DISALLOW NULL");
            }
        }

        return sb.toString();
    }
    /**
     * 构建添加列语句
     */
    @Override
    public String buildAddColumn(JQuickTableDefinition table, String tableName, JQuickColumnDefinition column) {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ").append(quoteIdentifier(table, tableName));
        sb.append(" ADD COLUMN ").append(quoteIdentifier(table, column.getColumnName()));
        sb.append(" ").append(getDataTypeString(table, column.getDataType()));
        if (!column.isNullable()) {
            sb.append(" NOT NULL");
        }
        if (column.getDefaultValue() != null && !column.isAutoIncrement()) {
            sb.append(" DEFAULT ").append(formatDefaultValue(column.getDefaultValue()));
        }
        return sb.toString();
    }

    /**
     * 构建删除列语句（Access 不支持，需要重建表）
     */
    @Override
    public String buildDropColumn(JQuickTableDefinition table, String tableName, String columnName) {
        StringBuilder sb = new StringBuilder();
        sb.append("-- Access does not support DROP COLUMN directly.\n");
        sb.append("-- Please recreate the table without the column: ").append(columnName);
        return sb.toString();
    }

    /**
     * 构建重命名列语句（Access 不支持，需要重建表）
     */
    public String buildRenameColumn(JQuickTableDefinition table, String tableName, String oldName, String newName) {
        StringBuilder sb = new StringBuilder();
        sb.append("-- Access does not support RENAME COLUMN directly.\n");
        sb.append("-- Please add a new column and copy data.");
        return sb.toString();
    }

    /**
     * 构建删除表语句
     */
    @Override
    public String buildDropTable(JQuickTableDefinition table, String tableName, boolean ifExists) {
        StringBuilder sb = new StringBuilder();
        if (ifExists) {
            // Access 没有 IF EXISTS，需要先检查
            sb.append("IF EXISTS (SELECT * FROM MSysObjects WHERE Name = '").append(tableName).append("') THEN\n");
            sb.append("  DROP TABLE ").append(quoteIdentifier(table, tableName)).append(";\n");
            sb.append("END IF");
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
        // Access 没有 TRUNCATE，使用 DELETE
        return "DELETE FROM " + quoteIdentifier(table, tableName);
    }

    /**
     * 构建重命名表语句
     */
    @Override
    public String buildRenameTable(JQuickTableDefinition table, String oldName, String newName) {
        return "ALTER TABLE " + quoteIdentifier(table, oldName) + " RENAME TO " + quoteIdentifier(table, newName);
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
        sb.append("PRIMARY KEY (").append(formatColumnList(table, columns)).append(")");
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
            return buildSelect(table, columns, whereClause) + (orderBy != null ? " ORDER BY " + orderBy : "");
        } else {
            // Access 分页需要使用子查询
            StringBuilder sql = new StringBuilder();
            String innerSelect = buildSelect(table, columns, whereClause);

            sql.append("SELECT * FROM (\n");
            sql.append("  SELECT TOP ").append(offset + limit).append(" * FROM (\n");
            sql.append("    ").append(innerSelect);
            if (orderBy != null && !orderBy.isEmpty()) {
                sql.append(" ORDER BY ").append(orderBy);
            }
            sql.append("\n  ) AS t1\n");
            sql.append("  ORDER BY ").append(getReverseOrderBy(orderBy));
            sql.append("\n) AS t2\n");
            sql.append("ORDER BY ").append(orderBy != null ? orderBy : "1");

            return sql.toString();
        }
    }

    /**
     * 获取反向排序（用于分页）
     */
    private String getReverseOrderBy(String orderBy) {
        if (orderBy == null || orderBy.isEmpty()) {
            return "1";
        }
        String[] parts = orderBy.split(",");
        List<String> reverseParts = new ArrayList<>();
        for (String part : parts) {
            String trimmed = part.trim();
            if (trimmed.toUpperCase().endsWith(" DESC")) {
                reverseParts.add(trimmed.substring(0, trimmed.length() - 5) + " ASC");
            } else if (trimmed.toUpperCase().endsWith(" ASC")) {
                reverseParts.add(trimmed.substring(0, trimmed.length() - 4) + " DESC");
            } else {
                reverseParts.add(trimmed + " DESC");
            }
        }
        return String.join(", ", reverseParts);
    }

    /**
     * 构建简单分页查询（使用 TOP）
     */
    public String buildTopPaginationQuery(JQuickTableDefinition table, List<String> columns,
                                          String whereClause, String orderBy, int limit) {
        if (table.getExtensions() == null) {
            table.setExtensions(new java.util.HashMap<>());
        }
        table.getExtensions().put("top", limit);
        return buildSelect(table, columns, whereClause) + (orderBy != null ? " ORDER BY " + orderBy : "");
    }

    /**
     * 构建查询分析语句
     */
    public String buildQueryAnalyze(String query) {
        return "-- Analyze query execution\n" + query;
    }

    /**
     * 构建紧凑数据库语句
     */
    public String buildCompactDatabase() {
        return "-- Use DAO or ADO to compact database\n-- CommandBars.ExecuteMso(\"CompactAndRepairDatabase\")";
    }

    /**
     * 构建修复数据库语句
     */
    public String buildRepairDatabase() {
        return "-- Use DAO or ADO to repair database\n-- DBEngine.CompactDatabase(source, destination)";
    }

    /**
     * 构建查询所有表语句
     */
    public String buildShowTables() {
        return "SELECT Name FROM MSysObjects WHERE Type = 1 AND Flags = 0";
    }

    /**
     * 构建查询所有列语句
     */
    public String buildShowColumns(String tableName) {
        return "SELECT Name, Type FROM MSysColumns WHERE Name = '" + tableName + "'";
    }

    /**
     * 构建查询所有索引语句
     */
    public String buildShowIndexes(String tableName) {
        return "SELECT Name FROM MSysObjects WHERE Type = 7 AND Name LIKE '" + tableName + "_%'";
    }
    /**
     * 构建参数化查询（使用 Parameters 对象）
     */
    public String buildParameterizedQuery(String query, List<String> parameters) {
        // Access 使用 ? 作为参数占位符
        String result = query;
        for (String param : parameters) {
            result = result.replaceFirst("\\?" + param + "\\?", "?");
        }
        return result;
    }

    /**
     * 构建查询提示
     */
    public String buildQueryHint(String query, String hint) {
        // Access 不支持查询提示
        return query;
    }
}
