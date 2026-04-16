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
import com.github.paohaijiao.connector.JQuickDataSourceConnector;
import com.github.paohaijiao.dataType.JQuickDataType;
import com.github.paohaijiao.dataType.JQuickDataTypeConverter;
import com.github.paohaijiao.dataType.impl.JQuickSQLServerDataTypeConverter;
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
 * SQL Server 方言实现
 * 支持 Microsoft SQL Server 数据库的 SQL 语法特性
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/15
 */
public class JQuickSQLServerDialect extends JQuickAbsSQLDialect {

    protected static final String SQLSERVER_QUOTE = "[";

    protected static final String SQLSERVER_QUOTE_END = "]";

    private static final String SEQ_SUFFIX = "_SEQ";

    @Override
    protected JQuickDataTypeConverter createDataTypeConvert() {
        return new JQuickSQLServerDataTypeConverter();
    }

    @Override
    protected String getQuoteKeyWord() {
        return SQLSERVER_QUOTE;
    }

    /**
     * SQL Server 使用方括号引用标识符
     */
    @Override
    protected String quoteIdentifier(JQuickTableDefinition tableDefinition, String identifier) {
        if (identifier == null || identifier.isEmpty()) {
            return identifier;
        }
        if ((identifier.startsWith(SQLSERVER_QUOTE) && identifier.endsWith(SQLSERVER_QUOTE_END)) ||
                (identifier.startsWith("\"") && identifier.endsWith("\""))) {
            return identifier;
        }
        if (tableDefinition != null && tableDefinition.isQuoteEnabled()) {
            return SQLSERVER_QUOTE + identifier + SQLSERVER_QUOTE_END;
        }
        return identifier;
    }

    @Override
    public String getAutoIncrementKeyword(JQuickTableDefinition tableDefinition) {
        // SQL Server 使用 IDENTITY
        return "IDENTITY";
    }

    @Override
    protected void appendTableOptions(StringBuilder sql, JQuickTableDefinition table) {
        if (table.getExtensions() != null) {
            if (table.getExtensions().containsKey("fileGroup")) {
                String fileGroup = table.getExtensions().get("fileGroup").toString();
                sql.append(" ON ").append(quoteIdentifier(table, fileGroup));
            }
            if (table.getExtensions().containsKey("textImageFileGroup")) {
                String textFileGroup = table.getExtensions().get("textImageFileGroup").toString();
                sql.append(" TEXTIMAGE_ON ").append(quoteIdentifier(table, textFileGroup));
            }
            if (table.getExtensions().containsKey("dataCompression")) {
                String compression = table.getExtensions().get("dataCompression").toString();
                sql.append(" WITH (DATA_COMPRESSION = ").append(compression).append(")");
            }
            if (table.getExtensions().containsKey("fileStream")) {
                String fileStream = table.getExtensions().get("fileStream").toString();
                sql.append(" FILESTREAM_ON ").append(quoteIdentifier(table, fileStream));
            }
        }
    }

    @Override
    public String getDriverClass(JQuickDataSourceConnector connector) {
        if (connector != null && connector.getDriverClass() != null && !connector.getDriverClass().trim().isEmpty()) {
            return connector.getDriverClass();
        }
        return "com.microsoft.sqlserver.jdbc.SQLServerDriver";
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
        String database = connector.getSchema();      // 数据库名
        String username = connector.getUsername();
        String password = connector.getPassword();
        if (host == null || host.trim().isEmpty()) {
            throw new IllegalStateException("Host is required for SQL Server connection");
        }
        String effectivePort = (port != null && !port.trim().isEmpty()) ? port : "1433";
        StringBuilder url = new StringBuilder();
        url.append("jdbc:sqlserver://").append(host).append(":").append(effectivePort);
        if (database != null && !database.trim().isEmpty()) {
            url.append(";databaseName=").append(database);
        }
        if (username != null && !username.trim().isEmpty()) {
            url.append(";user=").append(username);
        }

        if (password != null && !password.trim().isEmpty()) {
            url.append(";password=").append(password);
        }
        url.append(";encrypt=false");
        url.append(";trustServerCertificate=true");
        url.append(";loginTimeout=30");
        url.append(";socketTimeout=600");
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
                // 本地临时表使用 # 前缀
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
        if (table.getExtensions() != null && table.getExtensions().containsKey("globalTemporary")) {
            Boolean globalTemp = (Boolean) table.getExtensions().get("globalTemporary");
            if (globalTemp != null && globalTemp) {
                String tableName = table.getTableName();
                if (!tableName.startsWith("##")) {
                    table.setTableName("##" + tableName);
                }
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
        // 添加表注释（使用扩展属性）
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
     * 构建表注释语句（使用 sp_addextendedproperty）
     */
    protected String buildTableComment(JQuickTableDefinition table) {
        if (table.getComment() != null && !table.getComment().isEmpty()) {
            return "EXEC sp_addextendedproperty " +
                    "@name = N'MS_Description', " +
                    "@value = N'" + escapeString(table.getComment()) + "', " +
                    "@level0type = N'SCHEMA', @level0name = N'dbo', " +
                    "@level1type = N'TABLE', @level1name = N'" + table.getTableName() + "'";
        }
        return null;
    }

    /**
     * 构建列注释语句
     */
    public String buildColumnComment(JQuickTableDefinition table, String tableName, JQuickColumnDefinition column) {
        if (column.getComment() != null && !column.getComment().isEmpty()) {
            return "EXEC sp_addextendedproperty " +
                    "@name = N'MS_Description', " +
                    "@value = N'" + escapeString(column.getComment()) + "', " +
                    "@level0type = N'SCHEMA', @level0name = N'dbo', " +
                    "@level1type = N'TABLE', @level1name = N'" + tableName + "', " +
                    "@level2type = N'COLUMN', @level2name = N'" + column.getColumnName() + "'";
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
        def.append(getDataTypeString(table, column.getDataType()));
        if (column.isAutoIncrement()) {
            def.append(" ").append(getAutoIncrementKeyword(table));
            if (column.getExtensions() != null) {
                if (column.getExtensions().containsKey("identitySeed")) {
                    def.append("(").append(column.getExtensions().get("identitySeed"));
                    if (column.getExtensions().containsKey("identityIncrement")) {
                        def.append(", ").append(column.getExtensions().get("identityIncrement"));
                    }
                    def.append(")");
                }
            }
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
        // SQL Server 中 NULL 是默认值，不需要显式指定
    }

    @Override
    protected void appendDefaultClause(StringBuilder def, JQuickColumnDefinition column) {
        // 自增列不需要 DEFAULT
        if (column.isAutoIncrement()) {
            return;
        }
        if (column.getDefaultValue() != null) {
            def.append(" DEFAULT ").append(formatDefaultValue(column.getDefaultValue()));
        }
    }

    @Override
    protected void appendColumnComment(StringBuilder def, JQuickColumnDefinition column) {
        // SQL Server 使用扩展属性，不在列定义中
    }

    @Override
    protected String getSpecialDefaultKeyword(String value) {
        if (value == null) {
            return null;
        }
        String upperValue = value.toUpperCase();
        if ("CURRENT_TIMESTAMP".equals(upperValue) || "NOW()".equals(upperValue) || "GETDATE()".equals(upperValue)) {
            return "GETDATE()";
        }
        if ("CURRENT_DATE".equals(upperValue)) {
            return "CONVERT(DATE, GETDATE())";
        }
        if ("CURRENT_TIME".equals(upperValue)) {
            return "CONVERT(TIME, GETDATE())";
        }
        if ("GETUTCDATE".equals(upperValue)) {
            return "GETUTCDATE()";
        }
        if ("SYSDATETIME".equals(upperValue)) {
            return "SYSDATETIME()";
        }
        if ("NEWID".equals(upperValue)) {
            return "NEWID()";
        }
        if ("NEWSEQUENTIALID".equals(upperValue)) {
            return "NEWSEQUENTIALID()";
        }
        if ("CURRENT_USER".equals(upperValue)) {
            return "CURRENT_USER";
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
                return "NO ACTION"; // SQL Server 不支持 RESTRICT，使用 NO ACTION
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
        sb.append(" ").append(getDataTypeString(table, column.getDataType()));
        if (!column.isNullable()) {
            sb.append(" NOT NULL");
        }
        return sb.toString();
    }

    @Override
    public String buildChangeColumn(JQuickTableDefinition table, String tableName, String oldName, JQuickColumnDefinition newColumn) {
        // SQL Server 不支持直接修改列名，需要先重命名再修改
        StringBuilder sb = new StringBuilder();
        if (!oldName.equals(newColumn.getColumnName())) {
            sb.append("EXEC sp_rename '")
                    .append(quoteIdentifier(table, tableName)).append(".")
                    .append(quoteIdentifier(table, oldName)).append("', '")
                    .append(newColumn.getColumnName()).append("', 'COLUMN';\n");
        }
        sb.append("ALTER TABLE ").append(quoteIdentifier(table, tableName));
        sb.append(" ALTER COLUMN ").append(quoteIdentifier(table, newColumn.getColumnName()));
        sb.append(" ").append(getDataTypeString(table, newColumn.getDataType()));
        if (!newColumn.isNullable()) {
            sb.append(" NOT NULL");
        }
        return sb.toString();
    }

    @Override
    public String buildShowCreateTable(JQuickTableDefinition table, String tableName) {
        return "SELECT definition FROM sys.sql_modules " + "WHERE object_id = OBJECT_ID('" + tableName + "')";
    }

    @Override
    public String buildDescribeTable(JQuickTableDefinition table, String tableName) {
        return "SELECT " +
                "  c.name AS column_name, " +
                "  t.name AS data_type, " +
                "  c.is_nullable, " +
                "  c.column_default " +
                "FROM sys.columns c " +
                "INNER JOIN sys.types t ON c.user_type_id = t.user_type_id " +
                "WHERE c.object_id = OBJECT_ID('" + tableName + "') " +
                "ORDER BY c.column_id";
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
        // SQL Server 使用 OUTPUT 子句返回生成的主键
        if (!table.getPrimaryKeys().isEmpty() && hasAutoIncrement(table)) {
            String pkColumn = table.getPrimaryKeys().get(0).getColumns().get(0);
            sb.append(" OUTPUT INSERTED.").append(quoteIdentifier(table, pkColumn));
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
        if (index.getType() != null && "CLUSTERED".equalsIgnoreCase(index.getType())) {
            sb.append("CLUSTERED ");
        } else if (index.getType() != null && "NON_CLUSTERED".equalsIgnoreCase(index.getType())) {
            sb.append("NONCLUSTERED ");
        }
        sb.append("INDEX ").append(quoteIdentifier(table, index.getIndexName()));
        sb.append(" ON ").append("${tableName}").append(" (");
        sb.append(formatColumnList(table, index.getColumns()));
        sb.append(")");
        if (index.getExtensions() != null && index.getExtensions().containsKey("includeColumns")) {
            @SuppressWarnings("unchecked")
            List<String> includeColumns = (List<String>) index.getExtensions().get("includeColumns");
            if (includeColumns != null && !includeColumns.isEmpty()) {
                sb.append(" INCLUDE (");
                sb.append(formatColumnList(table, includeColumns));
                sb.append(")");
            }
        }
        if (index.getComment() != null && !index.getComment().isEmpty()) {
            sb.append(" WHERE ").append(index.getComment());
        }
        // 文件组
        if (index.getExtensions() != null && index.getExtensions().containsKey("fileGroup")) {
            String fileGroup = index.getExtensions().get("fileGroup").toString();
            sb.append(" ON ").append(quoteIdentifier(table, fileGroup));
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
        return "DROP SEQUENCE IF EXISTS " + quoteIdentifier(table, sequenceName);
    }

    /**
     * 构建获取序列下一个值语句
     */
    public String buildGetSequenceNextValue(String sequenceName) {
        return "SELECT NEXT VALUE FOR " + sequenceName;
    }

    /**
     * 构建获取序列当前值语句
     */
    public String buildGetSequenceCurrentValue(String sequenceName) {
        return "SELECT CURRENT_VALUE FROM SYS.SEQUENCES WHERE NAME = '" + sequenceName + "'";
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
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " DROP COLUMN " + quoteIdentifier(table, columnName);
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
        // SQL Server 需要先知道当前的数据类型
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " ALTER COLUMN " + quoteIdentifier(table, columnName) + " NOT NULL";
    }

    /**
     * 构建删除 NOT NULL 语句
     */
    public String buildDropNotNull(JQuickTableDefinition table, String tableName, String columnName) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " ALTER COLUMN " + quoteIdentifier(table, columnName) + " NULL";
    }

    /**
     * 构建设置默认值语句
     */
    public String buildSetDefaultValue(JQuickTableDefinition table, String tableName, String columnName, Object defaultValue) {
        // SQL Server 需要先删除旧的默认约束
        StringBuilder sb = new StringBuilder();
        sb.append("DECLARE @constraintName NVARCHAR(128)\n");
        sb.append("SELECT @constraintName = name FROM sys.default_constraints\n");
        sb.append("WHERE parent_object_id = OBJECT_ID('").append(tableName).append("')\n");
        sb.append("  AND parent_column_id = COLUMNPROPERTY(OBJECT_ID('").append(tableName).append("'), '")
                .append(columnName).append("', 'ColumnId')\n");
        sb.append("IF @constraintName IS NOT NULL\n");
        sb.append("  EXEC('ALTER TABLE ").append(quoteIdentifier(table, tableName))
                .append(" DROP CONSTRAINT ' + @constraintName)\n");
        sb.append("ALTER TABLE ").append(quoteIdentifier(table, tableName))
                .append(" ADD DEFAULT ").append(formatDefaultValue(defaultValue))
                .append(" FOR ").append(quoteIdentifier(table, columnName));
        return sb.toString();
    }

    /**
     * 构建删除默认值语句
     */
    public String buildDropDefaultValue(JQuickTableDefinition table, String tableName, String columnName) {
        return "DECLARE @constraintName NVARCHAR(128)\n" +
                "SELECT @constraintName = name FROM sys.default_constraints\n" +
                "WHERE parent_object_id = OBJECT_ID('" + tableName + "')\n" +
                "  AND parent_column_id = COLUMNPROPERTY(OBJECT_ID('" + tableName + "'), '" + columnName + "', 'ColumnId')\n" +
                "IF @constraintName IS NOT NULL\n" +
                "  EXEC('ALTER TABLE " + quoteIdentifier(table, tableName) + " DROP CONSTRAINT ' + @constraintName)";
    }

    /**
     * 构建删除表语句
     */
    @Override
    public String buildDropTable(JQuickTableDefinition table, String tableName, boolean ifExists) {
        StringBuilder sb = new StringBuilder();
        if (ifExists) {
            sb.append("IF OBJECT_ID('").append(tableName).append("', 'U') IS NOT NULL\n");
        }
        sb.append("DROP TABLE ").append(quoteIdentifier(table, tableName));
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
        sb.append(" ADD ");
        if (constraintName != null && !constraintName.isEmpty()) {
            sb.append("CONSTRAINT ").append(quoteIdentifier(table, constraintName)).append(" ");
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
        if (table.getExtensions() != null && table.getExtensions().containsKey("fileGroup")) {
            String fileGroup = table.getExtensions().get("fileGroup").toString();
            sb.append(" ON ").append(quoteIdentifier(table, fileGroup));
        }
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
     * 构建分页查询语句
     */
    public String buildPaginationQuery(JQuickTableDefinition table, List<String> columns, String whereClause, String orderBy, int offset, int limit) {
        StringBuilder sql = new StringBuilder();
        if (offset == 0) {
            sql.append(buildSelect(table, columns, whereClause));
            if (orderBy != null && !orderBy.isEmpty()) {
                sql.append(" ORDER BY ").append(orderBy);
            }
            sql.append(" OFFSET 0 ROWS FETCH NEXT ").append(limit).append(" ROWS ONLY");
        } else {
            sql.append("SELECT * FROM (\n");
            sql.append("  SELECT ROW_NUMBER() OVER (ORDER BY ").append(orderBy != null ? orderBy : "(SELECT NULL)").append(") AS row_num,\n");
            sql.append("    ").append(columns != null && !columns.isEmpty() ? columns.stream().map(c -> quoteIdentifier(table, c)).collect(Collectors.joining(", ")) : "*");
            sql.append("\n  FROM ").append(quoteIdentifier(table, table.getTableName()));
            if (whereClause != null && !whereClause.isEmpty()) {
                sql.append("\n  WHERE ").append(whereClause);
            }
            sql.append("\n) AS t\n");
            sql.append("WHERE row_num > ").append(offset).append(" AND row_num <= ").append(offset + limit);
        }
        return sql.toString();
    }

    /**
     * 构建 TOP 查询语句
     */
    public String buildTopQuery(JQuickTableDefinition table, List<String> columns, String whereClause, String orderBy, int top) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT TOP ").append(top).append(" ");
        if (columns == null || columns.isEmpty()) {
            sql.append("*");
        } else {
            sql.append(columns.stream().map(e -> quoteIdentifier(table, e)).collect(Collectors.joining(", ")));
        }
        sql.append(" FROM ").append(quoteIdentifier(table, table.getTableName()));
        if (whereClause != null && !whereClause.trim().isEmpty()) {
            sql.append(" WHERE ").append(whereClause);
        }
        if (orderBy != null && !orderBy.isEmpty()) {
            sql.append(" ORDER BY ").append(orderBy);
        }
        return sql.toString();
    }
    /**
     * 构建更新统计信息语句
     */
    public String buildUpdateStatistics(JQuickTableDefinition table, String tableName) {
        return "UPDATE STATISTICS " + quoteIdentifier(table, tableName);
    }

    /**
     * 构建重建索引语句
     */
    public String buildRebuildIndex(JQuickTableDefinition table, String tableName, String indexName) {
        if (indexName != null && !indexName.isEmpty()) {
            return "ALTER INDEX " + quoteIdentifier(table, indexName) + " ON " + quoteIdentifier(table, tableName) + " REBUILD";
        }
        return "ALTER INDEX ALL ON " + quoteIdentifier(table, tableName) + " REBUILD";
    }

    /**
     * 构建重组索引语句
     */
    public String buildReorganizeIndex(JQuickTableDefinition table, String tableName, String indexName) {
        if (indexName != null && !indexName.isEmpty()) {
            return "ALTER INDEX " + quoteIdentifier(table, indexName) + " ON " + quoteIdentifier(table, tableName) + " REORGANIZE";
        }
        return "ALTER INDEX ALL ON " + quoteIdentifier(table, tableName) + " REORGANIZE";
    }

    /**
     * 构建收缩数据库语句
     */
    public String buildShrinkDatabase(String databaseName) {
        return "DBCC SHRINKDATABASE (" + databaseName + ")";
    }
    /**
     * 构建表锁语句
     */
    public String buildTableLock(JQuickTableDefinition table, String tableName, String lockType) {
        return "SELECT * FROM " + quoteIdentifier(table, tableName) + " WITH (" + lockType + ") WHERE 1=0";
    }

    /**
     * 构建查询提示语句
     */
    public String buildQueryHint(String query, String hint) {
        return query + " OPTION (" + hint + ")";
    }
}