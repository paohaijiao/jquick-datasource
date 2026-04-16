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
import com.github.paohaijiao.dataType.impl.JQuickH2DataTypeConverter;
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
 * H2 数据库方言实现
 * 支持 H2 Database Engine 的 SQL 语法特性
 * H2 兼容多种模式（MySQL, PostgreSQL, Oracle 等），本实现基于 H2 原生模式
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/15
 */
public class JQuickH2Dialect extends JQuickAbsSQLDialect {

    protected static final String H2_QUOTE = "\"";

    private static final String SEQ_SUFFIX = "_SEQ";

    @Override
    protected JQuickDataTypeConverter createDataTypeConvert() {
        return new JQuickH2DataTypeConverter();
    }

    @Override
    protected String getQuoteKeyWord() {
        return H2_QUOTE;
    }

    @Override
    public String getAutoIncrementKeyword(JQuickTableDefinition tableDefinition) {
        return "IDENTITY";
    }

    @Override
    protected void appendTableOptions(StringBuilder sql, JQuickTableDefinition table) {
        if (table.getExtensions() != null) {
            // 存储引擎（H2 支持 MVStore 和 PageStore）
            if (table.getExtensions().containsKey("engine")) {
                String engine = table.getExtensions().get("engine").toString();
                sql.append(" ENGINE=").append(engine);
            }

            // 表类型（MEMORY, CACHE, 等）
            if (table.getExtensions().containsKey("tableType")) {
                String tableType = table.getExtensions().get("tableType").toString();
                sql.append(" ").append(tableType);
            }

            // 缓存大小
            if (table.getExtensions().containsKey("cacheSize")) {
                int cacheSize = ((Number) table.getExtensions().get("cacheSize")).intValue();
                sql.append(" CACHE SIZE ").append(cacheSize);
            }

            // 不记录日志（性能优化）
            if (table.getExtensions().containsKey("notLogged")) {
                Boolean notLogged = (Boolean) table.getExtensions().get("notLogged");
                if (notLogged != null && notLogged) {
                    sql.append(" NOT LOGGED");
                }
            }
        }
    }

    @Override
    public String getDriverClass(JQuickDataSourceConnector connector) {
        if (connector != null && connector.getDriverClass() != null && !connector.getDriverClass().trim().isEmpty()) {
            return connector.getDriverClass();
        }
        return "org.h2.Driver";
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
        String database = connector.getSchema();      // 数据库名/路径
        String username = connector.getUsername();
        String password = connector.getPassword();
        StringBuilder url = new StringBuilder();
        if (host != null && !host.trim().isEmpty()) {
            String effectivePort = (port != null && !port.trim().isEmpty()) ? port : "9092";
            url.append("jdbc:h2:tcp://").append(host).append(":").append(effectivePort);
            if (database != null && !database.trim().isEmpty()) {
                url.append("/").append(database);
            } else {
                url.append("/test");
            }

        } else {
            url.append("jdbc:h2:");
            if (database != null && !database.trim().isEmpty()) {
                if (database.startsWith("mem:") || database.equals("mem")) {
                    url.append(database);
                } else {
                    String dbPath = database.trim();
                    if (dbPath.contains("\\")) {
                        dbPath = dbPath.replace("\\", "/");
                    }
                    url.append(dbPath);
                }
            } else {
                url.append("mem:default");
            }
        }
        boolean hasParams = false;
        if (username != null && !username.trim().isEmpty()) {
            url.append(";USER=").append(username);
            hasParams = true;
        }
        if (password != null && !password.trim().isEmpty()) {
            url.append(";PASSWORD=").append(password);
            hasParams = true;
        }
        if (host == null || host.trim().isEmpty()) {
            if (!hasParams) {
                url.append(";DB_CLOSE_DELAY=-1");  // 保持数据库打开
                url.append(";MODE=MySQL");          // 兼容模式（可选）
            } else {
                url.append(";DB_CLOSE_DELAY=-1");
            }
        }

        return url.toString();
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
                sql.append("CREATE TEMPORARY TABLE ");
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
        // 临时表的事务选项
        if (isTemporary && table.getExtensions() != null) {
            if (table.getExtensions().containsKey("onCommitDeleteRows")) {
                Boolean deleteRows = (Boolean) table.getExtensions().get("onCommitDeleteRows");
                if (deleteRows != null && deleteRows) {
                    sql.append(" ON COMMIT DELETE ROWS");
                } else {
                    sql.append(" ON COMMIT PRESERVE ROWS");
                }
            }
        }
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
        // 自增列处理
        if (column.isAutoIncrement()) {
            def.append(getAutoIncrementKeyword(table));
            // 可选：指定起始值
            if (column.getDefaultValue() != null && column.getDefaultValue() instanceof Number) {
                def.append("(").append(column.getDefaultValue()).append(")");
            }
        } else {
            def.append(getDataTypeString(table, column.getDataType()));
        }
        appendNullClause(def, column);
        appendDefaultClause(def, column);
        appendColumnComment(def, column);
        appendColumnPosition(def, column);
        return def.toString();
    }

    @Override
    protected void appendNullClause(StringBuilder def, JQuickColumnDefinition column) {
        if (!column.isNullable()) {
            def.append(" NOT NULL");
        }
        // H2 中 NULL 是默认值，不需要显式指定
    }

    @Override
    protected void appendDefaultClause(StringBuilder def, JQuickColumnDefinition column) {
        // 自增列不需要 DEFAULT
        if (column.isAutoIncrement()) {
            return;
        }
        super.appendDefaultClause(def, column);
    }

    @Override
    protected void appendColumnComment(StringBuilder def, JQuickColumnDefinition column) {
        // H2 支持内联注释
        if (column.getComment() != null && !column.getComment().isEmpty()) {
            def.append(" COMMENT '").append(escapeString(column.getComment())).append("'");
        }
    }

    /**
     * 追加列位置（H2 支持 BEFORE/AFTER）
     */
    protected void appendColumnPosition(StringBuilder def, JQuickColumnDefinition column) {
        if (column.getPosition() != null && column.getPosition().getType() != null) {
            switch (column.getPosition().getType()) {
                case FIRST:
                    def.append(" BEFORE ").append(quoteIdentifier(null, "NULL"));
                    break;
                case AFTER:
                    if (column.getPosition().getAfterColumn() != null) {
                        def.append(" AFTER ").append(quoteIdentifier(null, column.getPosition().getAfterColumn()));
                    }
                    break;
            }
        }
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
        if ("RANDOM_UUID".equals(upperValue)) {
            return "RANDOM_UUID()";
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
        sb.append(" ").append(getDataTypeString(table, column.getDataType()));
        if (!column.isNullable()) {
            sb.append(" NOT NULL");
        } else {
            sb.append(" NULL");
        }

        if (column.getDefaultValue() != null) {
            sb.append(" DEFAULT ").append(formatDefaultValue(column.getDefaultValue()));
        }
        if (column.getComment() != null && !column.getComment().isEmpty()) {
            sb.append(" COMMENT '").append(escapeString(column.getComment())).append("'");
        }
        return sb.toString();
    }

    @Override
    public String buildChangeColumn(JQuickTableDefinition table, String tableName, String oldName, JQuickColumnDefinition newColumn) {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ").append(quoteIdentifier(table, tableName));
        if (!oldName.equals(newColumn.getColumnName())) {
            sb.append(" ALTER COLUMN ").append(quoteIdentifier(table, oldName));
            sb.append(" RENAME TO ").append(quoteIdentifier(table, newColumn.getColumnName()));
            sb.append(";\n");
            sb.append("ALTER TABLE ").append(quoteIdentifier(table, tableName));
        }
        sb.append(" ALTER COLUMN ").append(quoteIdentifier(table, newColumn.getColumnName()));
        sb.append(" ").append(getDataTypeString(table, newColumn.getDataType()));
        if (!newColumn.isNullable()) {
            sb.append(" NOT NULL");
        }
        if (newColumn.getDefaultValue() != null) {
            sb.append(" DEFAULT ").append(formatDefaultValue(newColumn.getDefaultValue()));
        }
        return sb.toString();
    }

    @Override
    public String buildShowCreateTable(JQuickTableDefinition table, String tableName) {
        return "SHOW COLUMNS FROM " + quoteIdentifier(table, tableName);
    }

    @Override
    public String buildDescribeTable(JQuickTableDefinition table, String tableName) {
        return "SELECT COLUMN_NAME, TYPE_NAME, NULLABLE, COLUMN_DEFAULT " + "FROM INFORMATION_SCHEMA.COLUMNS " +
                "WHERE TABLE_NAME = '" + tableName.toUpperCase() + "' " + "ORDER BY ORDINAL_POSITION";
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
        // H2 支持返回生成的主键
        if (!table.getPrimaryKeys().isEmpty() && hasAutoIncrement(table)) {
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
        // 索引类型
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
                case "SPATIAL":
                    sb.append("SPATIAL INDEX ").append(quoteIdentifier(table, index.getIndexName()));
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
        return "SELECT CURRVAL('" + sequenceName + "')";
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
     * 构建重命名列语句
     */
    public String buildRenameColumn(JQuickTableDefinition table, String tableName, String oldName, String newName) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " ALTER COLUMN " + quoteIdentifier(table, oldName) + " RENAME TO " + quoteIdentifier(table, newName);
    }

    /**
     * 构建设置 NOT NULL 语句
     */
    public String buildSetNotNull(JQuickTableDefinition table, String tableName, String columnName) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " ALTER COLUMN " + quoteIdentifier(table, columnName) + " SET NOT NULL";
    }

    /**
     * 构建删除 NOT NULL 语句
     */
    public String buildDropNotNull(JQuickTableDefinition table, String tableName, String columnName) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " ALTER COLUMN " + quoteIdentifier(table, columnName) + " SET NULL";
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
     * 构建删除表语句
     */
    @Override
    public String buildDropTable(JQuickTableDefinition table, String tableName, boolean ifExists) {
        StringBuilder sb = new StringBuilder("DROP TABLE ");
        if (ifExists) {
            sb.append("IF EXISTS ");
        }
        sb.append(quoteIdentifier(table, tableName));
        // 级联删除
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
        return "TRUNCATE TABLE " + quoteIdentifier(table, tableName);
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
        return "ALTER TABLE " + quoteIdentifier(table, tableName) +
                " DROP CONSTRAINT " + quoteIdentifier(table, constraintName);
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
     * 构建创建内存表语句
     */
    public String buildCreateMemoryTable(JQuickTableDefinition table) {
        if (table.getExtensions() == null) {
            table.setExtensions(new java.util.HashMap<>());
        }
        table.getExtensions().put("tableType", "MEMORY");
        return buildCreateTable(table);
    }

    /**
     * 构建分析表语句
     */
    public String buildAnalyzeTable(JQuickTableDefinition table, String tableName) {
        return "ANALYZE TABLE " + quoteIdentifier(table, tableName);
    }
    /**
     * 构建执行 SQL 脚本语句
     */
    public String buildRunScript(String scriptPath) {
        return "RUNSCRIPT FROM '" + scriptPath + "'";
    }

    /**
     * 构建导出 SQL 脚本语句
     */
    public String buildWriteScript(String scriptPath) {
        return "SCRIPT TO '" + scriptPath + "'";
    }

    /**
     * 构建备份语句
     */
    public String buildBackup(String backupPath) {
        return "BACKUP TO '" + backupPath + "'";
    }
}
