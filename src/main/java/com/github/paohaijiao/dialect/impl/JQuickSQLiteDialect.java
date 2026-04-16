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
import com.github.paohaijiao.dataType.impl.JQuickSQLiteDataTypeConverter;
import com.github.paohaijiao.dialect.JQuickAbsSQLDialect;
import com.github.paohaijiao.enums.JQuickForeignKeyAction;
import com.github.paohaijiao.enums.JQuickPositionType;
import com.github.paohaijiao.extra.JQuickForeignKeyConstraint;
import com.github.paohaijiao.extra.JQuickIndexDefinition;
import com.github.paohaijiao.row.JQuickRow;
import com.github.paohaijiao.table.JQuickTableDefinition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * SQLite 方言实现
 * 支持 SQLite 数据库的 SQL 语法特性
 *
 * 注意：SQLite 对 ALTER TABLE 支持有限，只支持：
 * - 重命名表
 * - 添加列
 * - 重命名列（3.25.0+）
 * 不支持：
 * - 删除列
 * - 修改列类型
 * - 添加/删除约束（除了重命名表）
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/15
 */
public class JQuickSQLiteDialect extends JQuickAbsSQLDialect {

    protected static final String SQLITE_QUOTE = "\"";

    @Override
    protected JQuickDataTypeConverter createDataTypeConvert() {
        return new JQuickSQLiteDataTypeConverter();
    }

    @Override
    protected String getQuoteKeyWord() {
        return SQLITE_QUOTE;
    }

    @Override
    public String getAutoIncrementKeyword(JQuickTableDefinition tableDefinition) {
        // SQLite 使用 AUTOINCREMENT 关键字
        return "AUTOINCREMENT";
    }

    @Override
    protected void appendTableOptions(StringBuilder sql, JQuickTableDefinition table) {
        if (table.getExtensions() != null) {
            // 临时表
            if (table.getExtensions().containsKey("temporary")) {
                Boolean temporary = (Boolean) table.getExtensions().get("temporary");
                if (temporary != null && temporary) {
                    // 临时表需要在 CREATE 语句中指定，这里标记，在 buildCreateTable 中处理
                }
            }
            // WITHOUT ROWID 表
            if (table.getExtensions().containsKey("withoutRowid")) {
                Boolean withoutRowid = (Boolean) table.getExtensions().get("withoutRowid");
                if (withoutRowid != null && withoutRowid) {
                    sql.append(" WITHOUT ROWID");
                }
            }
            // 严格模式 (3.37.0+)
            if (table.getExtensions().containsKey("strict")) {
                Boolean strict = (Boolean) table.getExtensions().get("strict");
                if (strict != null && strict) {
                    sql.append(" STRICT");
                }
            }
        }
    }

    @Override
    public String getDriverClass(JQuickDataSourceConnector connector) {
        if (connector != null && connector.getDriverClass() != null && !connector.getDriverClass().trim().isEmpty()) {
            return connector.getDriverClass();
        }
        return "org.sqlite.JDBC";
    }

    @Override
    public String getUrl(JQuickDataSourceConnector connector) {
        if (connector == null) {
            throw new IllegalArgumentException("Connector cannot be null");
        }
        if (connector.getUrl() != null && !connector.getUrl().trim().isEmpty()) {
            return connector.getUrl();
        }
        String host = connector.getHost();
        String database = connector.getSchema();
        String username = connector.getUsername();
        String password = connector.getPassword();
        StringBuilder url = new StringBuilder();
        url.append("jdbc:sqlite:");
        if (database != null && !database.trim().isEmpty()) {
            String dbPath = database.trim();
            if (dbPath.startsWith(":memory:")) {
                url.append(":memory:");
            } else if (dbPath.startsWith("::memory:")) {
                url.append("::memory:");
            } else if (dbPath.startsWith(":temp:")) {
                url.append(":temp:");
            } else if (dbPath.startsWith(":resource:")) {
                url.append(":resource:");
            } else {
                if (dbPath.contains("\\")) {
                    dbPath = dbPath.replace("\\", "/");
                }
                url.append(dbPath);
            }
        } else {
            url.append(":memory:");
        }
        boolean hasParams = false;
        if (password != null && !password.trim().isEmpty()) {
            url.append("?password=").append(password);
            hasParams = true;
        }
        if (!hasParams) {
            url.append("?");
            hasParams = true;
        } else {
            url.append("&");
        }
        url.append("foreign_keys=on");
        url.append("&journal_mode=WAL");
        url.append("&synchronous=NORMAL");
        return url.toString();
    }

    @Override
    public String buildCreateTable(JQuickTableDefinition table) {
        StringBuilder sql = new StringBuilder();
        boolean isTemporary = false;
        if (table.getExtensions() != null && table.getExtensions().containsKey("temporary")) {
            Boolean temporary = (Boolean) table.getExtensions().get("temporary");
            if (temporary != null && temporary) {
                isTemporary = true;
                sql.append("CREATE TEMP TABLE ");
            } else {
                sql.append("CREATE TABLE ");
            }
        } else {
            sql.append("CREATE TABLE ");
        }
        // IF NOT EXISTS
        if (table.getExtensions() != null && table.getExtensions().containsKey("ifNotExists")) {
            Boolean ifNotExists = (Boolean) table.getExtensions().get("ifNotExists");
            if (ifNotExists != null && ifNotExists) {
                sql.append("IF NOT EXISTS ");
            }
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
        if (table.getComment() != null && !table.getComment().isEmpty()) {
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
        if (column.isAutoIncrement()) {
            // SQLite 自增列必须是 INTEGER PRIMARY KEY
            def.append("INTEGER PRIMARY KEY ");
            if (column.isAutoIncrement()) {
                def.append(getAutoIncrementKeyword(table));
            }
        } else {
            def.append(getDataTypeString(table, column.getDataType()));
            appendNullClause(def, column);
            appendDefaultClause(def, column);
        }
        // 列注释（SQLite 不支持列注释，使用 SQL 注释）
        if (column.getComment() != null && !column.getComment().isEmpty()) {
            def.append(" /* ").append(escapeString(column.getComment())).append(" */");
        }

        return def.toString();
    }

    @Override
    protected void appendNullClause(StringBuilder def, JQuickColumnDefinition column) {
        if (!column.isNullable()) {
            def.append(" NOT NULL");
        }
        // SQLite 中 NULL 是默认值，不需要显式指定
    }

    @Override
    protected void appendDefaultClause(StringBuilder def, JQuickColumnDefinition column) {
        if (column.getDefaultValue() != null && !column.isAutoIncrement()) {
            def.append(" DEFAULT ").append(formatDefaultValue(column.getDefaultValue()));
        }
    }

    @Override
    protected void appendColumnComment(StringBuilder def, JQuickColumnDefinition column) {
        // SQLite 不支持内联注释，已在 buildColumnDefinition 中处理
    }

    /**
     * 追加列位置（SQLite 不支持 FIRST/AFTER）
     */
    @Override
    protected void appendColumnPosition(JQuickTableDefinition tableDefinition, StringBuilder def, JQuickColumnDefinition column) {
        // SQLite 不支持 FIRST/AFTER，忽略
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
    public String buildPrimaryKey(JQuickTableDefinition table, com.github.paohaijiao.extra.JQuickPrimaryKeyConstraint pk) {
        StringBuilder sb = new StringBuilder();
        if (pk.getConstraintName() != null && !pk.getConstraintName().isEmpty()) {
            sb.append("CONSTRAINT ").append(quoteIdentifier(table, pk.getConstraintName())).append(SPACE);
        }
        sb.append("PRIMARY KEY (");
        sb.append(formatColumnList(table, pk.getColumns()));
        sb.append(")");
        return sb.toString();
    }

    @Override
    public String buildUniqueConstraint(JQuickTableDefinition table, com.github.paohaijiao.extra.JQuickUniqueConstraint uc) {
        StringBuilder sb = new StringBuilder();
        if (uc.getConstraintName() != null && !uc.getConstraintName().isEmpty()) {
            sb.append("CONSTRAINT ").append(quoteIdentifier(table, uc.getConstraintName())).append(SPACE);
        }
        sb.append("UNIQUE (");
        sb.append(formatColumnList(table, uc.getColumns()));
        sb.append(")");
        return sb.toString();
    }

    @Override
    public String buildForeignKey(JQuickTableDefinition table, JQuickForeignKeyConstraint fk) {
        StringBuilder sb = new StringBuilder();
        if (fk.getConstraintName() != null && !fk.getConstraintName().isEmpty()) {
            sb.append("CONSTRAINT ").append(quoteIdentifier(table, fk.getConstraintName())).append(SPACE);
        }
        sb.append("FOREIGN KEY (");
        sb.append(formatColumnList(table, fk.getColumns()));
        sb.append(") REFERENCES ").append(quoteIdentifier(table, fk.getReferencedTable())).append(" (");
        sb.append(formatColumnList(table, fk.getReferencedColumns()));
        sb.append(")");
        if (fk.getOnDelete() != null) {
            sb.append(" ON DELETE ").append(convertForeignKeyAction(fk.getOnDelete()));
        }
        if (fk.getOnUpdate() != null) {
            sb.append(" ON UPDATE ").append(convertForeignKeyAction(fk.getOnUpdate()));
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
        sb.append("INDEX ");
        // IF NOT EXISTS
        if (index.hasExtension("ifNotExists")) {
            Boolean ifNotExists = index.getExtension("ifNotExists");
            if (ifNotExists != null && ifNotExists) {
                sb.append("IF NOT EXISTS ");
            }
        }
        sb.append(quoteIdentifier(table, index.getIndexName()));
        sb.append(" ON ").append("${tableName}").append(" (");
        // 支持排序方向
        List<String> indexedColumns = new ArrayList<>();
        if (index.getColumns() != null) {
            for (String col : index.getColumns()) {
                String direction = getColumnDirection(index, col);
                indexedColumns.add(quoteIdentifier(table, col) + direction);
            }
        }
        sb.append(String.join(", ", indexedColumns));
        sb.append(")");
        String whereCondition = index.getWhereCondition();
        if (whereCondition != null && !whereCondition.isEmpty()) {
            sb.append(" WHERE ").append(whereCondition);
        }

        return sb.toString();
    }

    /**
     * 获取列的排序方向
     */
    private String getColumnDirection(JQuickIndexDefinition index, String column) {
        if (index.hasExtension("collate")) {
            Map<String, String> collateMap = index.getExtension("collate");
            if (collateMap != null && collateMap.containsKey(column)) {
                return " COLLATE " + collateMap.get(column);
            }
        }
        if (index.hasExtension("order")) {
            Map<String, String> orderMap = index.getExtension("order");
            if (orderMap != null && orderMap.containsKey(column)) {
                String order = orderMap.get(column);
                if ("DESC".equalsIgnoreCase(order)) {
                    return " DESC";
                }
            }
        }
        return "";
    }

    @Override
    public String buildModifyColumn(JQuickTableDefinition table, String tableName, JQuickColumnDefinition column) {
        // SQLite 不支持直接修改列，需要重建表
        return buildRebuildTableForModify(table, tableName, column);
    }

    @Override
    public String buildChangeColumn(JQuickTableDefinition table, String tableName, String oldName, JQuickColumnDefinition newColumn) {
        // SQLite 3.25.0+ 支持重命名列，但不支持修改类型
        // 这里返回重命名语句，如果需要修改类型，需要重建表
        if (!oldName.equals(newColumn.getColumnName())) {
            return buildRenameColumn(table, tableName, oldName, newColumn.getColumnName());
        }
        // 如果只是修改类型，需要重建表
        return buildRebuildTableForModify(table, tableName, newColumn);
    }

    /**
     * 构建重建表以修改列定义
     */
    private String buildRebuildTableForModify(JQuickTableDefinition table, String tableName, JQuickColumnDefinition newColumn) {
        StringBuilder sb = new StringBuilder();
        String tempTableName = tableName + "_temp";
        // 1. 创建临时表
        sb.append("CREATE TABLE ").append(quoteIdentifier(table, tempTableName)).append(" AS SELECT * FROM ").append(quoteIdentifier(table, tableName)).append(";\n");
        // 2. 删除原表
        sb.append("DROP TABLE ").append(quoteIdentifier(table, tableName)).append(";\n");
        // 3. 创建新表（使用新的列定义）
        // 注意：这里需要完整的表定义，简化处理
        sb.append("-- 请手动重建表 ").append(tableName).append(" 并迁移数据\n");
        return sb.toString();
    }

    @Override
    public String buildShowCreateTable(JQuickTableDefinition table, String tableName) {
        return "SELECT sql FROM sqlite_master WHERE type='table' AND name='" + tableName + "'";
    }

    @Override
    public String buildDescribeTable(JQuickTableDefinition table, String tableName) {
        return "PRAGMA table_info(" + quoteIdentifier(table, tableName) + ")";
    }

    @Override
    public String buildInsert(JQuickTableDefinition table, JQuickRow row) {
        if (row == null || row.isEmpty() || table == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        // INSERT OR REPLACE / INSERT OR IGNORE
        if (table.getExtensions() != null && table.getExtensions().containsKey("conflict")) {
            String conflict = table.getExtensions().get("conflict").toString().toUpperCase();
            if ("REPLACE".equals(conflict)) {
                sb.append("INSERT OR REPLACE INTO ");
            } else if ("IGNORE".equals(conflict)) {
                sb.append("INSERT OR IGNORE INTO ");
            } else {
                sb.append("INSERT INTO ");
            }
        } else {
            sb.append("INSERT INTO ");
        }
        sb.append(quoteIdentifier(table, table.getTableName())).append(" (");
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

    /**
     * 构建 INSERT OR REPLACE 语句
     */
    public String buildInsertOrReplace(JQuickTableDefinition table, JQuickRow row) {
        if (table.getExtensions() == null) {
            table.setExtensions(new java.util.HashMap<>());
        }
        table.getExtensions().put("conflict", "REPLACE");
        return buildInsert(table, row);
    }

    /**
     * 构建 INSERT OR IGNORE 语句
     */
    public String buildInsertOrIgnore(JQuickTableDefinition table, JQuickRow row) {
        if (table.getExtensions() == null) {
            table.setExtensions(new java.util.HashMap<>());
        }
        table.getExtensions().put("conflict", "IGNORE");
        return buildInsert(table, row);
    }

    @Override
    public String buildUpdate(JQuickTableDefinition table, JQuickRow row, String whereClause) {
        if (row == null || row.isEmpty() || table == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE ");
        // UPDATE OR REPLACE / UPDATE OR IGNORE
        if (table.getExtensions() != null && table.getExtensions().containsKey("conflict")) {
            String conflict = table.getExtensions().get("conflict").toString().toUpperCase();
            if ("REPLACE".equals(conflict)) {
                sb.append("OR REPLACE ");
            } else if ("IGNORE".equals(conflict)) {
                sb.append("OR IGNORE ");
            }
        }
        sb.append(quoteIdentifier(table, table.getTableName())).append(" SET ");
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

    /**
     * 构建添加列语句（SQLite 支持）
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
        if (column.getDefaultValue() != null) {
            sb.append(" DEFAULT ").append(formatDefaultValue(column.getDefaultValue()));
        }
        return sb.toString();
    }

    /**
     * 构建删除列语句（SQLite 不支持，需要重建表）
     */
    @Override
    public String buildDropColumn(JQuickTableDefinition table, String tableName, String columnName) {
        // SQLite 不支持 DROP COLUMN，需要重建表
        return "-- SQLite does not support DROP COLUMN.\n" +
                "-- Please recreate the table without column: " + columnName + "\n" +
                buildRebuildTableForDropColumn(table, tableName, columnName);
    }

    /**
     * 构建重建表以删除列
     */
    private String buildRebuildTableForDropColumn(JQuickTableDefinition table, String tableName, String columnName) {
        StringBuilder sb = new StringBuilder();
        String tempTableName = tableName + "_temp";
        List<String> keepColumns = table.getColumns().stream()
                .filter(col -> !col.getColumnName().equals(columnName))
                .map(JQuickColumnDefinition::getColumnName)
                .collect(Collectors.toList());
        String keepColumnsStr = keepColumns.stream()
                .map(c -> quoteIdentifier(table, c))
                .collect(Collectors.joining(", "));
        sb.append("-- Step 1: Create temporary table\n");
        sb.append("CREATE TABLE ").append(quoteIdentifier(table, tempTableName)).append(" AS SELECT ")
                .append(keepColumnsStr).append(" FROM ").append(quoteIdentifier(table, tableName)).append(";\n");
        sb.append("-- Step 2: Drop original table\n");
        sb.append("DROP TABLE ").append(quoteIdentifier(table, tableName)).append(";\n");
        sb.append("-- Step 3: Rename temporary table\n");
        sb.append("ALTER TABLE ").append(quoteIdentifier(table, tempTableName))
                .append(" RENAME TO ").append(quoteIdentifier(table, tableName)).append(";\n");
        return sb.toString();
    }

    /**
     * 构建重命名列语句（SQLite 3.25.0+）
     */
    public String buildRenameColumn(JQuickTableDefinition table, String tableName, String oldName, String newName) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) +
                " RENAME COLUMN " + quoteIdentifier(table, oldName) +
                " TO " + quoteIdentifier(table, newName);
    }
    /**
     * 构建删除表语句
     */
    @Override
    public String buildDropTable(JQuickTableDefinition table, String tableName, boolean ifExists) {
        StringBuilder sb = new StringBuilder("DROP TABLE ");
        if (ifExists) {
            sb.append("IF EXISTS ");
        }
        sb.append(quoteIdentifier(table, tableName));
        return sb.toString();
    }

    /**
     * 构建清空表语句
     */
    @Override
    public String buildTruncateTable(JQuickTableDefinition table, String tableName) {
        // SQLite 没有 TRUNCATE，使用 DELETE
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
     * 构建 VACUUM 语句（优化数据库）
     */
    public String buildVacuum() {
        return "VACUUM";
    }

    /**
     * 构建 ANALYZE 语句（更新统计信息）
     */
    public String buildAnalyze() {
        return "ANALYZE";
    }

    /**
     * 构建 PRAGMA 语句
     */
    public String buildPragma(String pragmaName, Object value) {
        if (value == null) {
            return "PRAGMA " + pragmaName;
        }
        return "PRAGMA " + pragmaName + " = " + value;
    }

    /**
     * 构建外键约束检查开关
     */
    public String buildForeignKeyCheck(boolean enable) {
        return "PRAGMA foreign_keys = " + (enable ? "ON" : "OFF");
    }

    /**
     * 构建分页查询语句
     */
    public String buildPaginationQuery(JQuickTableDefinition table, List<String> columns, String whereClause,
                                       String orderBy, int offset, int limit) {
        StringBuilder sql = new StringBuilder();
        sql.append(buildSelect(table, columns, whereClause));
        if (orderBy != null && !orderBy.isEmpty()) {
            sql.append(" ORDER BY ").append(orderBy);
        }
        if (limit > 0) {
            sql.append(" LIMIT ").append(limit);
            if (offset > 0) {
                sql.append(" OFFSET ").append(offset);
            }
        }
        return sql.toString();
    }
    /**
     * 构建附加数据库语句
     */
    public String buildAttachDatabase(String databaseFile, String schemaName) {
        return "ATTACH DATABASE '" + databaseFile + "' AS " + quoteIdentifier(null, schemaName);
    }

    /**
     * 构建分离数据库语句
     */
    public String buildDetachDatabase(String schemaName) {
        return "DETACH DATABASE " + quoteIdentifier(null, schemaName);
    }

    /**
     * 构建备份语句
     */
    public String buildBackup(String sourceDatabase, String targetDatabase) {
        return "VACUUM INTO '" + targetDatabase + "'";
    }
    /**
     * 构建 EXPLAIN QUERY PLAN 语句
     */
    public String buildExplainQueryPlan(String query) {
        return "EXPLAIN QUERY PLAN " + query;
    }

    /**
     * 构建 EXPLAIN 语句
     */
    public String buildExplain(String query) {
        return "EXPLAIN " + query;
    }
}
