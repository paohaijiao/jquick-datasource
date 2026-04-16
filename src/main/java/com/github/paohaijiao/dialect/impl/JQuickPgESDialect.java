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
import com.github.paohaijiao.dataType.impl.JQuickPostgreSQLDataTypeConverter;
import com.github.paohaijiao.dialect.JQuickAbsSQLDialect;
import com.github.paohaijiao.enums.JQuickForeignKeyAction;
import com.github.paohaijiao.enums.JQuickIndexType;
import com.github.paohaijiao.extra.JQuickForeignKeyConstraint;
import com.github.paohaijiao.extra.JQuickIndexDefinition;
import com.github.paohaijiao.row.JQuickRow;
import com.github.paohaijiao.table.JQuickTableDefinition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * PostgreSQL 方言实现
 * 支持 PostgreSQL 数据库的 SQL 语法特性
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/15
 */
public class JQuickPgESDialect extends JQuickAbsSQLDialect {

    protected static final String PG_QUOTE = "\"";

    private static final String SEQ_SUFFIX = "_seq";

    @Override
    protected JQuickDataTypeConverter createDataTypeConvert() {
        return new JQuickPostgreSQLDataTypeConverter();
    }

    @Override
    protected String getQuoteKeyWord() {
        return PG_QUOTE;
    }

    @Override
    public String getAutoIncrementKeyword(JQuickTableDefinition tableDefinition) {
        return "SERIAL";
    }

    @Override
    protected void appendTableOptions(StringBuilder sql, JQuickTableDefinition table) {
        if (table.getExtensions() != null) {
            // 表空间
            if (table.getExtensions().containsKey("tablespace")) {
                String tablespace = table.getExtensions().get("tablespace").toString();
                sql.append(" TABLESPACE ").append(quoteIdentifier(table, tablespace));
            }
            // 表继承
            if (table.getExtensions().containsKey("inherits")) {
                String inherits = table.getExtensions().get("inherits").toString();
                sql.append(" INHERITS (").append(quoteIdentifier(table, inherits)).append(")");
            }
            // 日志级别
            if (table.getExtensions().containsKey("unlogged")) {
                Boolean unlogged = (Boolean) table.getExtensions().get("unlogged");
                if (unlogged != null && unlogged) {
                    // 需要在 CREATE TABLE 后添加 UNLOGGED 关键字
                    // 这里标记，实际在 buildCreateTable 中处理
                }
            }
        }
    }

    @Override
    public String getDriverClass(JQuickDataSourceConnector connector) {
        if (connector != null && connector.getDriverClass() != null && !connector.getDriverClass().trim().isEmpty()) {
            return connector.getDriverClass();
        }
        return "org.postgresql.Driver";
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
        String port = connector.getPort();
        String database = connector.getSchema();
        String username = connector.getUsername();
        String password = connector.getPassword();
        if (host == null || host.trim().isEmpty()) {
            throw new IllegalStateException("Host is required for PgES connection");
        }
        String effectivePort = (port != null && !port.trim().isEmpty()) ? port : "5432";
        String effectiveDatabase = (database != null && !database.trim().isEmpty()) ? database : "postgres";
        StringBuilder url = new StringBuilder();
        url.append("jdbc:postgresql://").append(host).append(":").append(effectivePort);
        url.append("/").append(effectiveDatabase);
        boolean hasParams = false;
        if (username != null && !username.trim().isEmpty()) {
            url.append("?user=").append(username);
            hasParams = true;
        }
        if (password != null && !password.trim().isEmpty()) {
            url.append(hasParams ? "&" : "?").append("password=").append(password);
            hasParams = true;
        }
        if (hasParams) {
            url.append("&useSSL=false");
            url.append("&stringtype=unspecified");
        } else {
            url.append("?useSSL=false");
            url.append("&stringtype=unspecified");
        }

        return url.toString();
    }

    @Override
    public String buildCreateTable(JQuickTableDefinition table) {
        StringBuilder sql = new StringBuilder();

        boolean unlogged = false;
        if (table.getExtensions() != null && table.getExtensions().containsKey("unlogged")) {
            Object unloggedObj = table.getExtensions().get("unlogged");
            if (unloggedObj instanceof Boolean && (Boolean) unloggedObj) {
                unlogged = true;
            }
        }

        sql.append("CREATE ");
        if (unlogged) {
            sql.append("UNLOGGED ");
        }
        sql.append("TABLE ").append(quoteIdentifier(table, table.getTableName())).append(" (\n");

        appendColumnDefinitions(sql, table);
        appendPrimaryKey(sql, table);
        appendUniqueConstraints(sql, table);
        appendForeignKeys(sql, table);
        appendCheckConstraints(sql, table);
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
            return "COMMENT ON TABLE " + quoteIdentifier(table, table.getTableName()) + " IS '" + escapeString(table.getComment()) + "'";
        }
        return null;
    }

    /**
     * 构建列注释语句
     */
    public String buildColumnComment(JQuickTableDefinition table, String tableName, JQuickColumnDefinition column) {
        if (column.getComment() != null && !column.getComment().isEmpty()) {
            return "COMMENT ON COLUMN " + quoteIdentifier(table, tableName) + "."
                    + quoteIdentifier(table, column.getColumnName())
                    + " IS '" + escapeString(column.getComment()) + "'";
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

        // 对于自增列，使用 SERIAL 或 IDENTITY
        if (column.isAutoIncrement()) {
            def.append(getAutoIncrementKeyword(table));
        } else {
            def.append(getDataTypeString(table, column.getDataType()));
        }

        appendNullClause(def, column);
        appendDefaultClause(def, column);
        appendColumnComment(def, column);

        return def.toString();
    }

    @Override
    protected void appendDefaultClause(StringBuilder def, JQuickColumnDefinition column) {
        // 对于自增列，不需要 DEFAULT
        if (column.isAutoIncrement()) {
            return;
        }
        super.appendDefaultClause(def, column);
    }

    @Override
    protected void appendNullClause(StringBuilder def, JQuickColumnDefinition column) {
        if (!column.isNullable()) {
            def.append(" NOT NULL");
        }
        // PostgreSQL 中 NULL 是默认值，不需要显式指定
    }

    @Override
    protected void appendColumnComment(StringBuilder def, JQuickColumnDefinition column) {
        // PostgreSQL 的列注释需要使用 COMMENT ON 语句单独添加
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
        if ("NULL".equals(upperValue)) {
            return "NULL";
        }
        return null;
    }

    @Override
    protected String formatBooleanDefault(boolean value) {
        return value ? "TRUE" : "FALSE";
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
    public String buildModifyColumn(JQuickTableDefinition table, String tableName, JQuickColumnDefinition column) {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ").append(quoteIdentifier(table, tableName));
        sb.append(" ALTER COLUMN ").append(quoteIdentifier(table, column.getColumnName()));
        sb.append(" TYPE ").append(getDataTypeString(table, column.getDataType()));
        if (column.getExtensions() != null && column.getExtensions().containsKey("using")) {
            String using = column.getExtensions().get("using").toString();
            sb.append(" USING ").append(using);
        }

        return sb.toString();
    }

    @Override
    public String buildChangeColumn(JQuickTableDefinition table, String tableName, String oldName, JQuickColumnDefinition newColumn) {
        StringBuilder sb = new StringBuilder();
        if (!oldName.equals(newColumn.getColumnName())) {
            sb.append("ALTER TABLE ").append(quoteIdentifier(table, tableName));
            sb.append(" RENAME COLUMN ").append(quoteIdentifier(table, oldName));
            sb.append(" TO ").append(quoteIdentifier(table, newColumn.getColumnName()));
            sb.append(";\n");
        }
        sb.append("ALTER TABLE ").append(quoteIdentifier(table, tableName));
        sb.append(" ALTER COLUMN ").append(quoteIdentifier(table, newColumn.getColumnName()));
        sb.append(" TYPE ").append(getDataTypeString(table, newColumn.getDataType()));
        return sb.toString();
    }

    @Override
    public String buildShowCreateTable(JQuickTableDefinition table, String tableName) {
        return "SELECT 'CREATE TABLE " + quoteIdentifier(table, tableName) + " (...)'";
    }

    @Override
    public String buildDescribeTable(JQuickTableDefinition table, String tableName) {
        return "SELECT column_name, data_type, is_nullable, column_default " +
                "FROM information_schema.columns " +
                "WHERE table_name = '" + tableName + "' " +
                "ORDER BY ordinal_position";
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
        if (!table.getPrimaryKeys().isEmpty()) {
            String pkColumn = table.getPrimaryKeys().get(0).getColumns().get(0);
            sb.append(" RETURNING ").append(quoteIdentifier(table, pkColumn));
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
        if (index.getType() != null) {
            switch (index.getType().toUpperCase()) {
                case "BTREE":
                    sb.append("INDEX ").append(quoteIdentifier(table, index.getIndexName()));
                    sb.append(" USING BTREE");
                    break;
                case "HASH":
                    sb.append("INDEX ").append(quoteIdentifier(table, index.getIndexName()));
                    sb.append(" USING HASH");
                    break;
                case "GIST":
                    sb.append("INDEX ").append(quoteIdentifier(table, index.getIndexName()));
                    sb.append(" USING GIST");
                    break;
                case "GIN":
                    sb.append("INDEX ").append(quoteIdentifier(table, index.getIndexName()));
                    sb.append(" USING GIN");
                    break;
                case "BRIN":
                    sb.append("INDEX ").append(quoteIdentifier(table, index.getIndexName()));
                    sb.append(" USING BRIN");
                    break;
                case "SPGIST":
                    sb.append("INDEX ").append(quoteIdentifier(table, index.getIndexName()));
                    sb.append(" USING SPGIST");
                    break;
                default:
                    sb.append("INDEX ").append(quoteIdentifier(table, index.getIndexName()));
                    break;
            }
        } else {
            sb.append("INDEX ").append(quoteIdentifier(table, index.getIndexName()));
        }
        sb.append(" ON ").append("${tableName}").append(" (");
        sb.append(formatColumnList(table, index.getColumns()));
        sb.append(")");
        if (index.getComment() != null && !index.getComment().isEmpty()) {
            sb.append(" WHERE ").append(index.getComment());
        }

        return sb.toString();
    }

    /**
     * 构建创建序列语句
     */
    public String buildCreateSequence(JQuickTableDefinition table, String sequenceName) {
        return "CREATE SEQUENCE IF NOT EXISTS " + quoteIdentifier(table, sequenceName);
    }

    /**
     * 构建创建自增列序列语句
     */
    public String buildCreateSequenceForColumn(JQuickTableDefinition table, String tableName, String columnName) {
        String seqName = tableName + "_" + columnName + SEQ_SUFFIX;
        return buildCreateSequence(table, seqName);
    }

    /**
     * 构建删除序列语句
     */
    public String buildDropSequence(JQuickTableDefinition table, String sequenceName) {
        return "DROP SEQUENCE IF EXISTS " + quoteIdentifier(table, sequenceName);
    }

    /**
     * 构建设置序列起始值语句
     */
    public String buildSetSequenceStartValue(JQuickTableDefinition table, String sequenceName, long startValue) {
        return "ALTER SEQUENCE " + quoteIdentifier(table, sequenceName) + " START WITH " + startValue;
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
     * 构建设置 NOT NULL 语句
     */
    public String buildSetNotNull(JQuickTableDefinition table, String tableName, String columnName) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) +
                " ALTER COLUMN " + quoteIdentifier(table, columnName) + " SET NOT NULL";
    }

    /**
     * 构建删除 NOT NULL 语句
     */
    public String buildDropNotNull(JQuickTableDefinition table, String tableName, String columnName) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) +
                " ALTER COLUMN " + quoteIdentifier(table, columnName) + " DROP NOT NULL";
    }

    /**
     * 构建设置默认值语句
     */
    public String buildSetDefaultValue(JQuickTableDefinition table, String tableName, String columnName, Object defaultValue) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " ALTER COLUMN " + quoteIdentifier(table, columnName) + " SET DEFAULT " + formatDefaultValue(defaultValue);
    }

    /**
     * 构建删除默认值语句
     */
    public String buildDropDefaultValue(JQuickTableDefinition table, String tableName, String columnName) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " ALTER COLUMN " + quoteIdentifier(table, columnName) + " DROP DEFAULT";
    }

    /**
     * 构建重命名列语句
     */
    public String buildRenameColumn(JQuickTableDefinition table, String tableName, String oldName, String newName) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " RENAME COLUMN " + quoteIdentifier(table, oldName) + " TO " + quoteIdentifier(table, newName);
    }

    /**
     * 构建添加列语句
     */
    @Override
    public String buildAddColumn(JQuickTableDefinition table, String tableName, JQuickColumnDefinition column) {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ").append(quoteIdentifier(table, tableName));
        sb.append(" ADD COLUMN ").append(buildColumnDefinition(table, column));
        return sb.toString();
    }

    /**
     * 构建删除列语句
     */
    @Override
    public String buildDropColumn(JQuickTableDefinition table, String tableName, String columnName) {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ").append(quoteIdentifier(table, tableName));
        sb.append(" DROP COLUMN ").append(quoteIdentifier(table, columnName));
        if (table.getExtensions() != null && table.getExtensions().containsKey("dropColumnCascade")) {
            Boolean cascade = (Boolean) table.getExtensions().get("dropColumnCascade");
            if (cascade != null && cascade) {
                sb.append(" CASCADE");
            }
        }
        return sb.toString();
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
        if (table.getExtensions() != null && table.getExtensions().containsKey("dropTableCascade")) {
            Boolean cascade = (Boolean) table.getExtensions().get("dropTableCascade");
            if (cascade != null && cascade) {
                sb.append(" CASCADE");
            }
        }

        return sb.toString();
    }

    /**
     * 构建清空表语句
     */
    @Override
    public String buildTruncateTable(JQuickTableDefinition table, String tableName) {
        StringBuilder sb = new StringBuilder("TRUNCATE TABLE ");
        sb.append(quoteIdentifier(table, tableName));
        if (table.getExtensions() != null && table.getExtensions().containsKey("truncateRestartIdentity")) {
            Boolean restart = (Boolean) table.getExtensions().get("truncateRestartIdentity");
            if (restart != null && restart) {
                sb.append(" RESTART IDENTITY");
            } else {
                sb.append(" CONTINUE IDENTITY");
            }
        }
        if (table.getExtensions() != null && table.getExtensions().containsKey("truncateCascade")) {
            Boolean cascade = (Boolean) table.getExtensions().get("truncateCascade");
            if (cascade != null && cascade) {
                sb.append(" CASCADE");
            }
        }
        return sb.toString();
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
        sb.append(" ADD ");
        if (constraintName != null && !constraintName.isEmpty()) {
            sb.append("CONSTRAINT ").append(quoteIdentifier(table, constraintName)).append(" ");
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
        sb.append(" ADD ");
        if (constraintName != null && !constraintName.isEmpty()) {
            sb.append("CONSTRAINT ").append(quoteIdentifier(table, constraintName)).append(" ");
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
        sb.append(" ADD ");
        if (fk.getConstraintName() != null && !fk.getConstraintName().isEmpty()) {
            sb.append("CONSTRAINT ").append(quoteIdentifier(table, fk.getConstraintName())).append(" ");
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
     * 构建 VACUUM 语句
     */
    public String buildVacuum(JQuickTableDefinition table, String tableName) {
        return "VACUUM " + quoteIdentifier(table, tableName);
    }

    /**
     * 构建 ANALYZE 语句
     */
    public String buildAnalyze(JQuickTableDefinition table, String tableName) {
        return "ANALYZE " + quoteIdentifier(table, tableName);
    }

    /**
     * 构建 VACUUM ANALYZE 语句
     */
    public String buildVacuumAnalyze(JQuickTableDefinition table, String tableName) {
        return "VACUUM ANALYZE " + quoteIdentifier(table, tableName);
    }

    /**
     * 构建 REINDEX 语句
     */
    public String buildReindex(JQuickTableDefinition table, String indexName) {
        return "REINDEX INDEX " + quoteIdentifier(table, indexName);
    }

    /**
     * 构建 CLUSTER 语句
     */
    public String buildCluster(JQuickTableDefinition table, String tableName, String indexName) {
        if (indexName != null && !indexName.isEmpty()) {
            return "CLUSTER " + quoteIdentifier(table, tableName) + " USING " + quoteIdentifier(table, indexName);
        }
        return "CLUSTER " + quoteIdentifier(table, tableName);
    }


    /**
     * 构建创建分区表语句
     */
    public String buildCreatePartitionedTable(JQuickTableDefinition table, String partitionType, String partitionKey) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE ").append(quoteIdentifier(table, table.getTableName()));
        sql.append(" (\n");
        appendColumnDefinitions(sql, table);
        sql.append(") PARTITION BY ").append(partitionType.toUpperCase());
        sql.append(" (").append(quoteIdentifier(table, partitionKey)).append(")");
        return sql.toString();
    }

    /**
     * 构建创建分区语句
     */
    public String buildCreatePartition(JQuickTableDefinition table, String tableName, String partitionName, String partitionValue) {
        return "CREATE TABLE " + quoteIdentifier(table, partitionName) + " PARTITION OF " + quoteIdentifier(table, tableName) + " FOR VALUES IN ('" + partitionValue + "')";
    }
}
