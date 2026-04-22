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
import com.github.paohaijiao.dataType.impl.JQuickOracleDataTypeConverter;
import com.github.paohaijiao.dialect.JQuickAbsSQLDialect;
import com.github.paohaijiao.enums.JQuickForeignKeyAction;
import com.github.paohaijiao.extra.JQuickForeignKeyConstraint;
import com.github.paohaijiao.extra.JQuickIndexDefinition;
import com.github.paohaijiao.extra.JQuickPrimaryKeyConstraint;
import com.github.paohaijiao.extra.JQuickUniqueConstraint;
import com.github.paohaijiao.statement.JQuickRow;
import com.github.paohaijiao.table.JQuickTableDefinition;

import java.text.SimpleDateFormat;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Oracle 方言实现
 * 支持 Oracle 数据库的 SQL 语法特性
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/15
 */
public class JQuickOracleDialect extends JQuickAbsSQLDialect {

    protected static final String ORACLE_QUOTE = "\"";

    private static final String SEQ_SUFFIX = "_SEQ";

    private static final String TRIGGER_SUFFIX = "_TRG";

    @Override
    protected JQuickDataTypeConverter createDataTypeConvert() {
        return new JQuickOracleDataTypeConverter();
    }

    @Override
    protected String getQuoteKeyWord() {
        return ORACLE_QUOTE;
    }

    @Override
    public String getAutoIncrementKeyword(JQuickTableDefinition tableDefinition) {
        return null;
    }

    @Override
    protected void appendTableOptions(StringBuilder sql, JQuickTableDefinition table) {
        if (table.getExtensions() != null) {
            if (table.getExtensions().containsKey("tablespace")) {
                String tablespace = table.getExtensions().get("tablespace").toString();
                sql.append(" TABLESPACE ").append(quoteIdentifier(table, tablespace));
            }
            if (table.getExtensions().containsKey("storage")) {
                @SuppressWarnings("unchecked")
                Map<String, Object> storage = (Map<String, Object>) table.getExtensions().get("storage");
                if (storage != null && !storage.isEmpty()) {
                    sql.append(" STORAGE (");
                    boolean first = true;
                    for (Map.Entry<String, Object> entry : storage.entrySet()) {
                        if (!first) {
                            sql.append(" ");
                        }
                        sql.append(entry.getKey()).append(" ").append(entry.getValue());
                        first = false;
                    }
                    sql.append(")");
                }
            }
            if (table.getExtensions().containsKey("logging")) {
                Boolean logging = (Boolean) table.getExtensions().get("logging");
                if (logging != null) {
                    sql.append(logging ? " LOGGING" : " NOLOGGING");
                }
            }
            if (table.getExtensions().containsKey("compress")) {
                Boolean compress = (Boolean) table.getExtensions().get("compress");
                if (compress != null && compress) {
                    sql.append(" COMPRESS");
                }
            }
        }
    }

    @Override
    public String getDriverClass(JQuickDataSourceConnector connector) {
        if (connector != null && connector.getDriverClass() != null && !connector.getDriverClass().trim().isEmpty()) {
            return connector.getDriverClass();
        }
        return "oracle.jdbc.OracleDriver";
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
            throw new IllegalStateException("Host is required for Oracle connection");
        }
        String effectivePort = (port != null && !port.trim().isEmpty()) ? port : "1521";
        String effectiveDatabase = (database != null && !database.trim().isEmpty()) ? database : "XE";
        String connectionType = connector.getByKeyStr("connectionType");
        boolean isServiceName = "service".equalsIgnoreCase(connectionType);
        StringBuilder url = new StringBuilder();
        url.append("jdbc:oracle:thin:@");
        if (isServiceName) {
            url.append("//").append(host).append(":").append(effectivePort);
            url.append("/").append(effectiveDatabase);
        } else {
            url.append(host).append(":").append(effectivePort);
            url.append(":").append(effectiveDatabase);
        }
        if (username != null && !username.trim().isEmpty() && password != null && !password.trim().isEmpty()) {
            String embeddedUrl = url.toString();
            url = new StringBuilder();
            url.append("jdbc:oracle:thin:").append(username).append("/").append(password);
            url.append("@").append(embeddedUrl.substring(embeddedUrl.indexOf("@") + 1));
        }

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
                sql.append("CREATE GLOBAL TEMPORARY TABLE ");
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
        if (isTemporary && table.getExtensions() != null) {
            if (table.getExtensions().containsKey("onCommitPreserveRows")) {
                Boolean preserveRows = (Boolean) table.getExtensions().get("onCommitPreserveRows");
                if (preserveRows != null && preserveRows) {
                    sql.append(" ON COMMIT PRESERVE ROWS");
                } else {
                    sql.append(" ON COMMIT DELETE ROWS");
                }
            }
        }
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
            return "COMMENT ON TABLE " + quoteIdentifier(table, table.getTableName()) +
                    " IS '" + escapeString(table.getComment()) + "'";
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
        def.append(getDataTypeString(table, column.getDataType()));
        if (!column.isNullable()) {
            def.append(" NOT NULL");
        }
        appendDefaultClause(def, column);
        return def.toString();
    }

    @Override
    protected void appendDefaultClause(StringBuilder def, JQuickColumnDefinition column) {
        if (column.getDefaultValue() != null) {
            def.append(" DEFAULT ").append(formatDefaultValue(column.getDefaultValue()));
        }
    }

    @Override
    protected void appendColumnComment(StringBuilder def, JQuickColumnDefinition column) {
        // Oracle 使用 COMMENT ON 语句，不在列定义中
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
        if ("SYSDATE".equals(upperValue)) {
            return "SYSDATE";
        }
        if ("SYSTIMESTAMP".equals(upperValue)) {
            return "SYSTIMESTAMP";
        }
        if ("USER".equals(upperValue)) {
            return "USER";
        }
        if ("NULL".equals(upperValue)) {
            return "NULL";
        }
        return null;
    }

    @Override
    protected String formatBooleanDefault(boolean value) {
        // Oracle 使用 'Y'/'N' 或 1/0，这里使用 1/0
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
    public String buildPrimaryKey(JQuickTableDefinition table, JQuickPrimaryKeyConstraint pk) {
        StringBuilder sb = new StringBuilder();
        if (pk.getConstraintName() != null && !pk.getConstraintName().isEmpty()) {
            sb.append("CONSTRAINT ").append(quoteIdentifier(table, pk.getConstraintName())).append(SPACE);
        }
        sb.append("PRIMARY KEY (");
        sb.append(formatColumnList(table, pk.getColumns()));
        sb.append(")");
        // 主键使用 USING INDEX 子句
        if (table.getExtensions() != null && table.getExtensions().containsKey("primaryKeyUsingIndex")) {
            String usingIndex = table.getExtensions().get("primaryKeyUsingIndex").toString();
            sb.append(" USING INDEX ").append(usingIndex);
        }
        return sb.toString();
    }

    @Override
    public String buildUniqueConstraint(JQuickTableDefinition table, JQuickUniqueConstraint uc) {
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
        if (index.getType() != null && "BITMAP".equalsIgnoreCase(index.getType())) {
            sb.append("BITMAP ");
        }
        sb.append("INDEX ").append(quoteIdentifier(table, index.getIndexName()));
        sb.append(" ON ").append("${tableName}").append(" (");
        sb.append(formatColumnList(table, index.getColumns()));
        sb.append(")");
        // 索引表空间
        if (table.getExtensions() != null && table.getExtensions().containsKey("indexTablespace")) {
            String tablespace = table.getExtensions().get("indexTablespace").toString();
            sb.append(" TABLESPACE ").append(quoteIdentifier(table, tablespace));
        }
        // 本地分区索引
        if (index.getType() != null && "LOCAL".equalsIgnoreCase(index.getType())) {
            sb.append(" LOCAL");
        }
        // 全局分区索引
        if (index.getType() != null && "GLOBAL".equalsIgnoreCase(index.getType())) {
            sb.append(" GLOBAL");
        }
        return sb.toString();
    }

    @Override
    public String buildModifyColumn(JQuickTableDefinition table, String tableName, JQuickColumnDefinition column) {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ").append(quoteIdentifier(table, tableName));
        sb.append(" MODIFY ").append(quoteIdentifier(table, column.getColumnName()));
        sb.append(" ").append(getDataTypeString(table, column.getDataType()));
        if (!column.isNullable()) {
            sb.append(" NOT NULL");
        } else {
            sb.append(" NULL");
        }
        if (column.getDefaultValue() != null) {
            sb.append(" DEFAULT ").append(formatDefaultValue(column.getDefaultValue()));
        }
        return sb.toString();
    }

    @Override
    public String buildChangeColumn(JQuickTableDefinition table, String tableName, String oldName, JQuickColumnDefinition newColumn) {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ").append(quoteIdentifier(table, tableName));
        if (!oldName.equals(newColumn.getColumnName())) {
            sb.append(" RENAME COLUMN ").append(quoteIdentifier(table, oldName));
            sb.append(" TO ").append(quoteIdentifier(table, newColumn.getColumnName()));
            sb.append(";\n");
            sb.append("ALTER TABLE ").append(quoteIdentifier(table, tableName));
        }
        sb.append(" MODIFY ").append(quoteIdentifier(table, newColumn.getColumnName()));
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
        return "SELECT DBMS_METADATA.GET_DDL('TABLE', '" + tableName.toUpperCase() + "') FROM DUAL";
    }

    @Override
    public String buildDescribeTable(JQuickTableDefinition table, String tableName) {
        return "SELECT COLUMN_NAME, DATA_TYPE, NULLABLE, DATA_DEFAULT " +
                "FROM USER_TAB_COLUMNS " +
                "WHERE TABLE_NAME = '" + tableName.toUpperCase() + "' " +
                "ORDER BY COLUMN_ID";
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
        // Oracle 使用 RETURNING INTO 子句
        if (!table.getPrimaryKeys().isEmpty() && hasAutoIncrement(table)) {
            String pkColumn = table.getPrimaryKeys().get(0).getColumns().get(0);
            sb.append(" RETURNING ").append(quoteIdentifier(table, pkColumn)).append(" INTO ?");
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
                    sb.append(" NOCYCLE");
                }
            }
        }
        return sb.toString();
    }

    /**
     * 构建为自增列创建序列的语句
     */
    public String buildCreateSequenceForColumn(JQuickTableDefinition table, String tableName, String columnName) {
        String seqName = (tableName + "_" + columnName + SEQ_SUFFIX).toUpperCase();
        return buildCreateSequence(table, seqName);
    }

    /**
     * 构建删除序列语句
     */
    public String buildDropSequence(JQuickTableDefinition table, String sequenceName) {
        return "DROP SEQUENCE " + quoteIdentifier(table, sequenceName);
    }

    /**
     * 构建获取序列当前值语句
     */
    public String buildGetSequenceCurrentValue(String sequenceName) {
        return "SELECT " + sequenceName + ".CURRVAL FROM DUAL";
    }

    /**
     * 构建获取序列下一个值语句
     */
    public String buildGetSequenceNextValue(String sequenceName) {
        return "SELECT " + sequenceName + ".NEXTVAL FROM DUAL";
    }

    /**
     * 构建为自增列创建触发器的语句
     */
    public String buildCreateTriggerForAutoIncrement(JQuickTableDefinition table, String tableName, String columnName, String sequenceName) {
        StringBuilder sb = new StringBuilder();
        String triggerName = (tableName + "_" + columnName + TRIGGER_SUFFIX).toUpperCase();
        String tableNameUpper = tableName.toUpperCase();
        String columnNameUpper = columnName.toUpperCase();
        String seqNameUpper = sequenceName.toUpperCase();
        sb.append("CREATE OR REPLACE TRIGGER ").append(triggerName).append("\n");
        sb.append("  BEFORE INSERT ON ").append(tableNameUpper).append("\n");
        sb.append("  FOR EACH ROW\n");
        sb.append("BEGIN\n");
        sb.append("  IF :NEW.").append(columnNameUpper).append(" IS NULL THEN\n");
        sb.append("    SELECT ").append(seqNameUpper).append(".NEXTVAL INTO :NEW.").append(columnNameUpper).append(" FROM DUAL;\n");
        sb.append("  END IF;\n");
        sb.append("END;");
        return sb.toString();
    }

    /**
     * 构建删除触发器语句
     */
    public String buildDropTrigger(JQuickTableDefinition table, String triggerName) {
        return "DROP TRIGGER " + quoteIdentifier(table, triggerName);
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
        if (!column.isNullable()) {
            sb.append(" NOT NULL");
        }
        if (column.getDefaultValue() != null) {
            sb.append(" DEFAULT ").append(formatDefaultValue(column.getDefaultValue()));
        }
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
                sb.append(" CASCADE CONSTRAINTS");
            }
        }

        return sb.toString();
    }

    /**
     * 构建设置列不可用语句
     */
    public String buildSetColumnUnused(JQuickTableDefinition table, String tableName, String columnName) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " SET UNUSED COLUMN " + quoteIdentifier(table, columnName);
    }

    /**
     * 构建删除未使用的列语句
     */
    public String buildDropUnusedColumns(JQuickTableDefinition table, String tableName) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " DROP UNUSED COLUMNS";
    }

    /**
     * 构建重命名列语句
     */
    public String buildRenameColumn(JQuickTableDefinition table, String tableName, String oldName, String newName) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " RENAME COLUMN " + quoteIdentifier(table, oldName) + " TO " + quoteIdentifier(table, newName);
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
        StringBuilder sb = new StringBuilder("DROP TABLE ");
        if (ifExists) {
            // Oracle 没有 IF EXISTS，需要先检查
            sb.append(quoteIdentifier(table, tableName));
        } else {
            sb.append(quoteIdentifier(table, tableName));
        }
        // 级联删除约束
        if (table.getExtensions() != null && table.getExtensions().containsKey("dropTableCascade")) {
            Boolean cascade = (Boolean) table.getExtensions().get("dropTableCascade");
            if (cascade != null && cascade) {
                sb.append(" CASCADE CONSTRAINTS");
            }
        }
        // 清除回收站
        if (table.getExtensions() != null && table.getExtensions().containsKey("purge")) {
            Boolean purge = (Boolean) table.getExtensions().get("purge");
            if (purge != null && purge) {
                sb.append(" PURGE");
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
        // 重用存储
        if (table.getExtensions() != null && table.getExtensions().containsKey("reuseStorage")) {
            Boolean reuse = (Boolean) table.getExtensions().get("reuseStorage");
            if (reuse != null && reuse) {
                sb.append(" REUSE STORAGE");
            } else {
                sb.append(" DROP STORAGE");
            }
        }
        // 级联
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
        return "RENAME " + quoteIdentifier(table, oldName) + " TO " + quoteIdentifier(table, newName);
    }

    /**
     * 构建移动表到新表空间语句
     */
    public String buildMoveTable(JQuickTableDefinition table, String tableName, String newTablespace) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " MOVE TABLESPACE " + quoteIdentifier(table, newTablespace);
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

        return sb.toString();
    }

    /**
     * 构建添加检查约束语句
     */
    public String buildAddCheckConstraint(JQuickTableDefinition table, String tableName, String constraintName, String condition) {
        StringBuilder sb = new StringBuilder("ALTER TABLE ");
        sb.append(quoteIdentifier(table, tableName));
        sb.append(" ADD ");
        if (constraintName != null && !constraintName.isEmpty()) {
            sb.append("CONSTRAINT ").append(quoteIdentifier(table, constraintName)).append(" ");
        }
        sb.append("CHECK (").append(condition).append(")");
        return sb.toString();
    }

    /**
     * 构建启用约束语句
     */
    public String buildEnableConstraint(JQuickTableDefinition table, String tableName, String constraintName) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " ENABLE CONSTRAINT " + quoteIdentifier(table, constraintName);
    }

    /**
     * 构建禁用约束语句
     */
    public String buildDisableConstraint(JQuickTableDefinition table, String tableName, String constraintName) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " DISABLE CONSTRAINT " + quoteIdentifier(table, constraintName);
    }

    /**
     * 构建删除约束语句
     */
    public String buildDropConstraint(JQuickTableDefinition table, String tableName, String constraintName) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " DROP CONSTRAINT " + quoteIdentifier(table, constraintName);
    }


    /**
     * 构建创建范围分区表语句
     */
    public String buildCreateRangePartitionedTable(JQuickTableDefinition table, String partitionKey, List<Map<String, Object>> partitions) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE ").append(quoteIdentifier(table, table.getTableName()));
        sql.append(" (\n");
        appendColumnDefinitions(sql, table);
        sql.append(") PARTITION BY RANGE (").append(quoteIdentifier(table, partitionKey)).append(") (\n");
        for (int i = 0; i < partitions.size(); i++) {
            Map<String, Object> partition = partitions.get(i);
            String partitionName = (String) partition.get("name");
            Object lessThan = partition.get("lessThan");
            sql.append(INDENT).append("PARTITION ").append(quoteIdentifier(table, partitionName));
            sql.append(" VALUES LESS THAN (").append(formatValue(lessThan)).append(")");
            if (i < partitions.size() - 1) {
                sql.append(COMMA);
            }
            sql.append(NEW_LINE);
        }

        sql.append(")");
        return sql.toString();
    }

    /**
     * 构建添加分区语句
     */
    public String buildAddPartition(JQuickTableDefinition table, String tableName, String partitionName, Object lessThanValue) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " ADD PARTITION " + quoteIdentifier(table, partitionName) + " VALUES LESS THAN (" + formatValue(lessThanValue) + ")";
    }

    /**
     * 构建删除分区语句
     */
    public String buildDropPartition(JQuickTableDefinition table, String tableName, String partitionName) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " DROP PARTITION " + quoteIdentifier(table, partitionName);
    }

    /**
     * 构建截断分区语句
     */
    public String buildTruncatePartition(JQuickTableDefinition table, String tableName, String partitionName) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " TRUNCATE PARTITION " + quoteIdentifier(table, partitionName);
    }


    /**
     * 构建分页查询语句
     */
    public String buildPaginationQuery(JQuickTableDefinition table, List<String> columns, String whereClause, String orderBy, int offset, int limit) {
        StringBuilder sql = new StringBuilder();
        // Oracle 12c+ 使用 OFFSET FETCH
        sql.append(buildSelect(table, columns, whereClause));
        if (orderBy != null && !orderBy.isEmpty()) {
            sql.append(" ORDER BY ").append(orderBy);
        }
        if (offset >= 0 && limit > 0) {
            sql.append(" OFFSET ").append(offset).append(" ROWS");
            sql.append(" FETCH NEXT ").append(limit).append(" ROWS ONLY");
        }

        return sql.toString();
    }

    /**
     * 构建传统分页查询（兼容 Oracle 11g 及以下）
     */
    public String buildLegacyPaginationQuery(JQuickTableDefinition table, List<String> columns, String whereClause, String orderBy, int offset, int limit) {
        StringBuilder sql = new StringBuilder();
        String innerSelect = buildSelect(table, columns, whereClause);
        if (orderBy != null && !orderBy.isEmpty()) {
            innerSelect += " ORDER BY " + orderBy;
        }
        int endRow = offset + limit;
        sql.append("SELECT * FROM (\n");
        sql.append("  SELECT ROWNUM AS rn, t.* FROM (\n");
        sql.append("    ").append(innerSelect).append("\n");
        sql.append("  ) t\n");
        sql.append("  WHERE ROWNUM <= ").append(endRow).append("\n");
        sql.append(") WHERE rn > ").append(offset);

        return sql.toString();
    }

    /**
     * 构建收集表统计信息语句
     */
    public String buildGatherTableStats(JQuickTableDefinition table, String tableName) {
        return "BEGIN\n" + "  DBMS_STATS.GATHER_TABLE_STATS(ownname => USER, tabname => '" + tableName.toUpperCase() + "');\n" + "END;";
    }

    /**
     * 构建分析表语句
     */
    public String buildAnalyzeTable(JQuickTableDefinition table, String tableName) {
        return "ANALYZE TABLE " + quoteIdentifier(table, tableName) + " COMPUTE STATISTICS";
    }
}