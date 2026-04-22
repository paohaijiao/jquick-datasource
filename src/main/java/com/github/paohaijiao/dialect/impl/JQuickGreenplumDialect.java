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
import com.github.paohaijiao.dataType.JQuickDataTypeConverter;
import com.github.paohaijiao.dataType.impl.JQuickGreenplumDataTypeConverter;
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
 * Greenplum 方言实现
 * 支持 Greenplum 分布式 MPP 数据库的 SQL 语法特性
 * <p>
 * Greenplum 版本兼容性：
 * - Greenplum 5.x (基于 PostgreSQL 8.3)
 * - Greenplum 6.x (基于 PostgreSQL 9.4)
 * - Greenplum 7.x (基于 PostgreSQL 12)
 * <p>
 * Greenplum 与传统数据库的差异：
 * - 基于 PostgreSQL，语法高度兼容
 * - 支持分布式存储和计算
 * - 支持分区表和列存储
 * - 支持外部表（gpfdist、S3 等）
 * - 支持资源队列和资源组
 * - 适合大规模数据仓库和 OLAP 场景
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/15
 */
public class JQuickGreenplumDialect extends JQuickAbsSQLDialect {

    protected static final String GREENPLUM_QUOTE = "\"";

    private static final String SEQ_SUFFIX = "_seq";

    @Override
    protected JQuickDataTypeConverter createDataTypeConvert() {
        return new JQuickGreenplumDataTypeConverter();
    }

    @Override
    protected String getQuoteKeyWord() {
        return GREENPLUM_QUOTE;
    }

    @Override
    public String getAutoIncrementKeyword(JQuickTableDefinition tableDefinition) {
        // Greenplum 支持 SERIAL 和 BIGSERIAL
        return "SERIAL";
    }

    @Override
    protected void appendTableOptions(StringBuilder sql, JQuickTableDefinition table) {
        // Greenplum 表选项 - 核心特性
        if (table.getExtensions() != null) {
            // 存储格式
            if (table.getExtensions().containsKey("appendOnly")) {
                Boolean appendOnly = (Boolean) table.getExtensions().get("appendOnly");
                if (appendOnly != null && appendOnly) {
                    sql.append("\nWITH (APPENDONLY=true");

                    // 压缩
                    if (table.getExtensions().containsKey("compress")) {
                        String compress = table.getExtensions().get("compress").toString();
                        sql.append(", COMPRESSLEVEL=").append(compress);
                    }
                    // 压缩类型
                    if (table.getExtensions().containsKey("compresstype")) {
                        String compresstype = table.getExtensions().get("compresstype").toString();
                        sql.append(", COMPRESSTYPE=").append(compresstype);
                    }

                    // 块大小
                    if (table.getExtensions().containsKey("blocksize")) {
                        Integer blocksize = (Integer) table.getExtensions().get("blocksize");
                        if (blocksize != null && blocksize > 0) {
                            sql.append(", BLOCKSIZE=").append(blocksize);
                        }
                    }
                    // 列存储
                    if (table.getExtensions().containsKey("orientation")) {
                        String orientation = table.getExtensions().get("orientation").toString();
                        if ("column".equalsIgnoreCase(orientation)) {
                            sql.append(", ORIENTATION=COLUMN");
                        }
                    }

                    sql.append(")");
                }
            }

            // 分布策略（Greenplum 核心特性）
            if (table.getExtensions().containsKey("distributedBy")) {
                @SuppressWarnings("unchecked")
                List<String> distributedColumns = (List<String>) table.getExtensions().get("distributedBy");
                if (distributedColumns != null && !distributedColumns.isEmpty()) {
                    sql.append("\nDISTRIBUTED BY (");
                    sql.append(distributedColumns.stream()
                            .map(c -> quoteIdentifier(table, c))
                            .collect(Collectors.joining(", ")));
                    sql.append(")");
                }
            } else if (table.getExtensions().containsKey("distributedRandomly")) {
                Boolean random = (Boolean) table.getExtensions().get("distributedRandomly");
                if (random != null && random) {
                    sql.append("\nDISTRIBUTED RANDOMLY");
                }
            } else if (table.getExtensions().containsKey("distributedReplicated")) {
                Boolean replicated = (Boolean) table.getExtensions().get("distributedReplicated");
                if (replicated != null && replicated) {
                    sql.append("\nDISTRIBUTED REPLICATED");
                }
            } else {
                // 默认使用第一个列作为分布键
                if (table.getColumns() != null && !table.getColumns().isEmpty()) {
                    sql.append("\nDISTRIBUTED BY (").append(quoteIdentifier(table, table.getColumns().get(0).getColumnName())).append(")");
                }
            }

            // 分区
            if (table.getExtensions().containsKey("partitionBy")) {
                String partitionBy = table.getExtensions().get("partitionBy").toString();
                sql.append("\nPARTITION BY ").append(partitionBy);
            }

            // 表空间
            if (table.getExtensions().containsKey("tablespace")) {
                String tablespace = table.getExtensions().get("tablespace").toString();
                sql.append("\nTABLESPACE ").append(quoteIdentifier(table, tablespace));
            }

            // 表注释
            if (table.getComment() != null && !table.getComment().isEmpty()) {
                sql.append(";\nCOMMENT ON TABLE ").append(quoteIdentifier(table, table.getTableName()))
                        .append(" IS '").append(escapeString(table.getComment())).append("'");
            }

            // 填充因子
            if (table.getExtensions().containsKey("fillfactor")) {
                Integer fillfactor = (Integer) table.getExtensions().get("fillfactor");
                if (fillfactor != null && fillfactor > 0) {
                    sql.append("\nWITH (FILLFACTOR=").append(fillfactor).append(")");
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
        String database = connector.getSchema();      // 数据库名
        String username = connector.getUsername();
        String password = connector.getPassword();
        if (host == null || host.trim().isEmpty()) {
            throw new IllegalStateException("Host is required for Greenplum connection");
        }
        String effectivePort = (port != null && !port.trim().isEmpty()) ? port : "5432";
        StringBuilder url = new StringBuilder();
        url.append("jdbc:postgresql://").append(host).append(":").append(effectivePort);
        if (database != null && !database.trim().isEmpty()) {
            url.append("/").append(database);
        } else {
            url.append("/");
        }
        boolean hasParams = false;
        if (database != null && !database.isEmpty()) {
            url.append("?prepareThreshold=0");
            hasParams = true;
        }
        if (username != null && !username.trim().isEmpty()) {
            url.append(hasParams ? "&" : "?").append("user=").append(username);
            hasParams = true;
        }
        if (password != null && !password.trim().isEmpty()) {
            url.append(hasParams ? "&" : "?").append("password=").append(password);
            hasParams = true;
        }
        if (hasParams) {
            url.append("&ApplicationName=JQuickDataSource");
            url.append("&reWriteBatchedInserts=true");
        } else {
            url.append("?ApplicationName=JQuickDataSource");
            url.append("&reWriteBatchedInserts=true");
        }

        return url.toString();
    }

    @Override
    public String buildCreateTable(JQuickTableDefinition table) {
        StringBuilder sql = new StringBuilder();
        // 可写外部表
        boolean isWritableExternal = false;
        if (table.getExtensions() != null && table.getExtensions().containsKey("writableExternal")) {
            Boolean writable = (Boolean) table.getExtensions().get("writableExternal");
            if (writable != null && writable) {
                isWritableExternal = true;
                sql.append("CREATE WRITABLE EXTERNAL TABLE ");
            }
        } else if (table.getExtensions() != null && table.getExtensions().containsKey("external")) {
            Boolean external = (Boolean) table.getExtensions().get("external");
            if (external != null && external) {
                sql.append("CREATE EXTERNAL TABLE ");
            } else {
                sql.append("CREATE TABLE ");
            }
        } else {
            sql.append("CREATE TABLE ");
        }

        // 临时表
        if (table.getExtensions() != null && table.getExtensions().containsKey("temporary")) {
            Boolean temporary = (Boolean) table.getExtensions().get("temporary");
            if (temporary != null && temporary) {
                sql.insert(7, "TEMP ");
            }
        }

        // IF NOT EXISTS
        if (table.getExtensions() != null && table.getExtensions().containsKey("ifNotExists")) {
            Boolean ifNotExists = (Boolean) table.getExtensions().get("ifNotExists");
            if (ifNotExists != null && ifNotExists) {
                sql.append("IF NOT EXISTS ");
            }
        }

        sql.append(quoteIdentifier(table, table.getTableName()));

        // 外部表的定位
        if (table.getExtensions() != null && (table.getExtensions().containsKey("external") ||
                table.getExtensions().containsKey("writableExternal"))) {
            if (table.getExtensions().containsKey("location")) {
                String location = table.getExtensions().get("location").toString();
                sql.append("\nLOCATION ('").append(escapeString(location)).append("')");
            }

            // 外部表格式
            if (table.getExtensions().containsKey("format")) {
                String format = table.getExtensions().get("format").toString();
                sql.append("\nFORMAT '").append(format.toUpperCase()).append("'");

                // CSV 选项
                if ("CSV".equalsIgnoreCase(format)) {
                    if (table.getExtensions().containsKey("delimiter")) {
                        String delimiter = table.getExtensions().get("delimiter").toString();
                        sql.append(" (DELIMITER '").append(delimiter).append("')");
                    }
                    if (table.getExtensions().containsKey("quote")) {
                        String quote = table.getExtensions().get("quote").toString();
                        sql.append(" (QUOTE '").append(quote).append("')");
                    }
                    if (table.getExtensions().containsKey("escape")) {
                        String escape = table.getExtensions().get("escape").toString();
                        sql.append(" (ESCAPE '").append(escape).append("')");
                    }
                }
            }

            // 外部表错误处理
            if (table.getExtensions().containsKey("logErrors")) {
                String logErrors = table.getExtensions().get("logErrors").toString();
                sql.append("\nLOG ERRORS ").append(logErrors);
            }

            // 外部表分段
            if (table.getExtensions().containsKey("segmentRejectLimit")) {
                String limit = table.getExtensions().get("segmentRejectLimit").toString();
                sql.append("\nSEGMENT REJECT LIMIT ").append(limit);
            }
        } else {
            // 普通表的列定义
            sql.append(" (\n");
            List<JQuickColumnDefinition> columns = table.getColumns();
            for (int i = 0; i < columns.size(); i++) {
                JQuickColumnDefinition column = columns.get(i);
                sql.append(INDENT).append(quoteIdentifier(table, column.getColumnName()))
                        .append(" ").append(getDataTypeString(table, column.getDataType()));

                // 列注释
                if (column.getComment() != null && !column.getComment().isEmpty()) {
                    sql.append(" COMMENT '").append(escapeString(column.getComment())).append("'");
                }

                // 默认值
                if (column.getDefaultValue() != null) {
                    sql.append(" DEFAULT ").append(formatDefaultValue(column.getDefaultValue()));
                }

                if (i < columns.size() - 1) {
                    sql.append(",");
                }
                sql.append(NEW_LINE);
            }
            sql.append(")");
        }

        appendTableOptions(sql, table);

        return sql.toString();
    }

    /**
     * 构建创建分区表语句
     */
    public String buildCreatePartitionedTable(JQuickTableDefinition table, String partitionType, String partitionKey, List<Map<String, Object>> partitions) {
        StringBuilder sql = new StringBuilder();

        sql.append(buildCreateTable(table));

        if (partitionType != null && partitionKey != null) {
            sql.append("\nPARTITION BY ").append(partitionType.toUpperCase());
            sql.append(" (").append(quoteIdentifier(table, partitionKey)).append(")");
            sql.append("\n(");
            for (int i = 0; i < partitions.size(); i++) {
                Map<String, Object> partition = partitions.get(i);
                String partitionName = (String) partition.get("name");
                String startValue = (String) partition.get("start");
                String endValue = (String) partition.get("end");

                sql.append("\n  PARTITION ").append(quoteIdentifier(table, partitionName));
                sql.append(" START ('").append(startValue).append("')");
                sql.append(" END ('").append(endValue).append("')");

                if (partition.containsKey("every")) {
                    sql.append(" EVERY (").append(partition.get("every")).append(")");
                }

                if (i < partitions.size() - 1) {
                    sql.append(",");
                }
            }

            sql.append("\n)");
        }

        return sql.toString();
    }

    /**
     * 构建创建外部表语句（gpfdist）
     */
    public String buildCreateExternalTableWithGpfdist(JQuickTableDefinition table, String gpfdistUrl, String format) {
        if (table.getExtensions() == null) {
            table.setExtensions(new java.util.HashMap<>());
        }
        table.getExtensions().put("external", true);
        table.getExtensions().put("location", "gpfdist://" + gpfdistUrl);
        table.getExtensions().put("format", format);
        return buildCreateTable(table);
    }

    @Override
    public String buildColumnDefinition(JQuickTableDefinition table, JQuickColumnDefinition column) {
        StringBuilder def = new StringBuilder();
        def.append(quoteIdentifier(table, column.getColumnName())).append(SPACE);
        // 自增
        if (column.isAutoIncrement()) {
            def.append(getAutoIncrementKeyword(table));
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
        // Greenplum 使用 COMMENT 语句，不在列定义中
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
        if ("RANDOM".equals(upperValue)) {
            return "RANDOM()";
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
    public String buildPrimaryKey(JQuickTableDefinition table,
                                  com.github.paohaijiao.extra.JQuickPrimaryKeyConstraint pk) {
        String sb = "CONSTRAINT " + quoteIdentifier(table, pk.getConstraintName()) +
                " PRIMARY KEY (" +
                formatColumnList(table, pk.getColumns()) +
                ")";
        return sb;
    }

    @Override
    public String buildUniqueConstraint(JQuickTableDefinition table, com.github.paohaijiao.extra.JQuickUniqueConstraint uc) {
        String sb = "CONSTRAINT " + quoteIdentifier(table, uc.getConstraintName()) +
                " UNIQUE (" +
                formatColumnList(table, uc.getColumns()) +
                ")";
        return sb;
    }

    @Override
    public String buildForeignKey(JQuickTableDefinition table, JQuickForeignKeyConstraint fk) {
        StringBuilder sb = new StringBuilder();
        sb.append("CONSTRAINT ").append(quoteIdentifier(table, fk.getConstraintName()))
                .append(" FOREIGN KEY (");
        sb.append(formatColumnList(table, fk.getColumns()));
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

    @Override
    public String buildModifyColumn(JQuickTableDefinition table, String tableName, JQuickColumnDefinition column) {
        // Greenplum 基于 PostgreSQL，使用 ALTER TABLE ALTER COLUMN
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ").append(quoteIdentifier(table, tableName));
        sb.append(" ALTER COLUMN ").append(quoteIdentifier(table, column.getColumnName()));
        sb.append(" TYPE ").append(getDataTypeString(table, column.getDataType()));
        if (column.getDefaultValue() != null && !column.isAutoIncrement()) {
            sb.append(",\nALTER TABLE ").append(quoteIdentifier(table, tableName));
            sb.append(" ALTER COLUMN ").append(quoteIdentifier(table, column.getColumnName()));
            sb.append(" SET DEFAULT ").append(formatDefaultValue(column.getDefaultValue()));
        }

        if (!column.isNullable()) {
            sb.append(",\nALTER TABLE ").append(quoteIdentifier(table, tableName));
            sb.append(" ALTER COLUMN ").append(quoteIdentifier(table, column.getColumnName()));
            sb.append(" SET NOT NULL");
        }

        return sb.toString();
    }

    @Override
    public String buildChangeColumn(JQuickTableDefinition table, String tableName, String oldName, JQuickColumnDefinition newColumn) {
        StringBuilder sb = new StringBuilder();

        // 重命名列
        if (!oldName.equals(newColumn.getColumnName())) {
            sb.append("ALTER TABLE ").append(quoteIdentifier(table, tableName));
            sb.append(" RENAME COLUMN ").append(quoteIdentifier(table, oldName));
            sb.append(" TO ").append(quoteIdentifier(table, newColumn.getColumnName()));
            sb.append(";\n");
        }

        // 修改类型
        sb.append("ALTER TABLE ").append(quoteIdentifier(table, tableName));
        sb.append(" ALTER COLUMN ").append(quoteIdentifier(table, newColumn.getColumnName()));
        sb.append(" TYPE ").append(getDataTypeString(table, newColumn.getDataType()));
        if (newColumn.getDefaultValue() != null && !newColumn.isAutoIncrement()) {
            sb.append(";\nALTER TABLE ").append(quoteIdentifier(table, tableName));
            sb.append(" ALTER COLUMN ").append(quoteIdentifier(table, newColumn.getColumnName()));
            sb.append(" SET DEFAULT ").append(formatDefaultValue(newColumn.getDefaultValue()));
        }
        if (!newColumn.isNullable()) {
            sb.append(";\nALTER TABLE ").append(quoteIdentifier(table, tableName));
            sb.append(" ALTER COLUMN ").append(quoteIdentifier(table, newColumn.getColumnName()));
            sb.append(" SET NOT NULL");
        }
        return sb.toString();
    }

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

        if (column.getComment() != null && !column.getComment().isEmpty()) {
            sb.append(";\nCOMMENT ON COLUMN ").append(quoteIdentifier(table, tableName))
                    .append(".").append(quoteIdentifier(table, column.getColumnName()))
                    .append(" IS '").append(escapeString(column.getComment())).append("'");
        }

        return sb.toString();
    }

    @Override
    public String buildDropColumn(JQuickTableDefinition table, String tableName, String columnName) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " DROP COLUMN " + quoteIdentifier(table, columnName);
    }

    @Override
    public String buildShowCreateTable(JQuickTableDefinition table, String tableName) {
        return "SELECT 'CREATE TABLE " + tableName + " (...)'";
    }

    @Override
    public String buildDescribeTable(JQuickTableDefinition table, String tableName) {
        return "SELECT " +
                "  a.attname AS column_name, " +
                "  pg_catalog.format_type(a.atttypid, a.atttypmod) AS data_type, " +
                "  a.attnotnull AS not_null, " +
                "  d.adsrc AS default_value " +
                "FROM pg_catalog.pg_attribute a " +
                "LEFT JOIN pg_catalog.pg_attrdef d ON (a.attrelid = d.adrelid AND a.attnum = d.adnum) " +
                "WHERE a.attrelid = '" + tableName + "'::regclass " +
                "  AND a.attnum > 0 " +
                "  AND NOT a.attisdropped " +
                "ORDER BY a.attnum";
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
        // 返回自增主键
        if (!table.getPrimaryKeys().isEmpty() && hasAutoIncrement(table)) {
            String pkColumn = table.getPrimaryKeys().get(0).getColumns().get(0);
            sb.append(" RETURNING ").append(quoteIdentifier(table, pkColumn));
        }

        return sb.toString();
    }

    /**
     * 构建批量插入语句
     */
    public String buildInsertBatch(JQuickTableDefinition table, List<JQuickRow> rows) {
        if (rows == null || rows.isEmpty() || table == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(quoteIdentifier(table, table.getTableName()));
        List<String> columns = new ArrayList<>(rows.get(0).keySet());
        sb.append(" (").append(columns.stream()
                        .map(e -> quoteIdentifier(table, e))
                        .collect(Collectors.joining(", ")))
                .append(") VALUES ");

        for (int i = 0; i < rows.size(); i++) {
            JQuickRow row = rows.get(i);
            if (i > 0) {
                sb.append(", ");
            }
            sb.append("(");
            List<String> values = new ArrayList<>();
            for (String col : columns) {
                values.add(formatValue(row.get(col)));
            }
            sb.append(String.join(", ", values));
            sb.append(")");
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
        sb.append("INDEX ").append(quoteIdentifier(table, index.getIndexName()));
        sb.append(" ON ").append("${tableName}").append(" (");
        sb.append(formatColumnList(table, index.getColumns()));
        sb.append(")");
        // 索引类型
        if (index.getType() != null) {
            String upperType = index.getType().toUpperCase();
            if ("BTREE".equals(upperType) || "HASH".equals(upperType) ||
                    "GIST".equals(upperType) || "GIN".equals(upperType)) {
                sb.append(" USING ").append(upperType);
            }
        }
        // 部分索引
        if (index.getWhereCondition() != null && !index.getWhereCondition().isEmpty()) {
            sb.append(" WHERE ").append(index.getWhereCondition());
        }
        // 表空间
        if (index.getFileGroup() != null && !index.getFileGroup().isEmpty()) {
            sb.append(" TABLESPACE ").append(quoteIdentifier(table, index.getFileGroup()));
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
        return "SELECT NEXTVAL('" + sequenceName + "')";
    }

    /**
     * 构建获取序列当前值语句
     */
    public String buildGetSequenceCurrentValue(String sequenceName) {
        return "SELECT CURRVAL('" + sequenceName + "')";
    }

    /**
     * 构建添加分区语句
     */
    public String buildAddPartition(JQuickTableDefinition table, String tableName,
                                    String partitionName, String startValue, String endValue) {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ").append(quoteIdentifier(table, tableName));
        sb.append(" ADD PARTITION ").append(quoteIdentifier(table, partitionName));
        sb.append(" START ('").append(startValue).append("')");
        sb.append(" END ('").append(endValue).append("')");

        if (table.getExtensions() != null && table.getExtensions().containsKey("partitionEvery")) {
            sb.append(" EVERY (").append(table.getExtensions().get("partitionEvery")).append(")");
        }

        return sb.toString();
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
     * 构建交换分区语句
     */
    public String buildExchangePartition(JQuickTableDefinition table, String tableName, String partitionName, String newTableName) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " EXCHANGE PARTITION " + quoteIdentifier(table, partitionName) + " WITH TABLE " + quoteIdentifier(table, newTableName);
    }

    /**
     * 构建拆分分区语句
     */
    public String buildSplitPartition(JQuickTableDefinition table, String tableName, String partitionName, String splitPoint) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " SPLIT PARTITION " + quoteIdentifier(table, partitionName) + " AT ('" + splitPoint + "')";
    }

    /**
     * 构建重命名分区语句
     */
    public String buildRenamePartition(JQuickTableDefinition table, String tableName, String oldName, String newName) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " RENAME PARTITION " + quoteIdentifier(table, oldName) + " TO " + quoteIdentifier(table, newName);
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
        if (table.getExtensions() != null && table.getExtensions().containsKey("cascade")) {
            Boolean cascade = (Boolean) table.getExtensions().get("cascade");
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
     * 构建移动表到新表空间语句
     */
    public String buildMoveTable(JQuickTableDefinition table, String tableName, String newTablespace) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " SET TABLESPACE " + quoteIdentifier(table, newTablespace);
    }

    /**
     * 构建更改表分布策略语句
     */
    public String buildSetDistributedBy(JQuickTableDefinition table, String tableName, List<String> columns) {
        String sb = "ALTER TABLE " + quoteIdentifier(table, tableName) +
                " SET DISTRIBUTED BY (" +
                columns.stream().map(c -> quoteIdentifier(table, c)).collect(Collectors.joining(", ")) +
                ")";
        return sb;
    }

    /**
     * 构建更改表分布策略为随机
     */
    public String buildSetDistributedRandomly(JQuickTableDefinition table, String tableName) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " SET DISTRIBUTED RANDOMLY";
    }

    /**
     * 构建更改表分布策略为复制
     */
    public String buildSetDistributedReplicated(JQuickTableDefinition table, String tableName) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " SET DISTRIBUTED REPLICATED";
    }

    /**
     * 构建扩容表语句
     */
    public String buildExpandTable(JQuickTableDefinition table, String tableName, int newBuckets) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) +
                " EXPAND TABLE TO " + newBuckets + " SEGMENTS";
    }
    /**
     * 构建创建资源队列语句
     */
    public String buildCreateResourceQueue(String queueName, Map<String, String> options) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE RESOURCE QUEUE ").append(queueName);
        if (options != null && !options.isEmpty()) {
            sb.append(" WITH (");
            boolean first = true;
            for (Map.Entry<String, String> entry : options.entrySet()) {
                if (!first) {
                    sb.append(", ");
                }
                sb.append(entry.getKey()).append("=").append(entry.getValue());
                first = false;
            }
            sb.append(")");
        }

        return sb.toString();
    }

    /**
     * 构建删除资源队列语句
     */
    public String buildDropResourceQueue(String queueName) {
        return "DROP RESOURCE QUEUE " + queueName;
    }

    /**
     * 构建修改资源队列语句
     */
    public String buildAlterResourceQueue(String queueName, Map<String, String> options) {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER RESOURCE QUEUE ").append(queueName);
        if (options != null && !options.isEmpty()) {
            sb.append(" WITH (");
            boolean first = true;
            for (Map.Entry<String, String> entry : options.entrySet()) {
                if (!first) {
                    sb.append(", ");
                }
                sb.append(entry.getKey()).append("=").append(entry.getValue());
                first = false;
            }
            sb.append(")");
        }

        return sb.toString();
    }

    /**
     * 构建查询计划语句
     */
    public String buildExplain(String query) {
        return "EXPLAIN " + query;
    }

    /**
     * 构建详细查询计划语句
     */
    public String buildExplainAnalyze(String query) {
        return "EXPLAIN ANALYZE " + query;
    }

    /**
     * 构建分析查询语句
     */
    public String buildAnalyzeQuery(String query) {
        return "EXPLAIN ANALYZE " + query;
    }

    /**
     * 构建 gpfdist 外部表查询语句
     */
    public String buildSelectFromGpfdist(String tableName) {
        return "SELECT * FROM " + tableName;
    }

    /**
     * 构建数据导出到文件语句
     */
    public String buildCopyToFile(String tableName, String filePath) {
        return "COPY " + tableName + " TO '" + filePath + "' WITH (FORMAT CSV, HEADER)";
    }

    /**
     * 构建从文件导入数据语句
     */
    public String buildCopyFromFile(String tableName, String filePath) {
        return "COPY " + tableName + " FROM '" + filePath + "' WITH (FORMAT CSV, HEADER)";
    }


    /**
     * 构建查询表分布信息语句
     */
    public String buildGetTableDistribution(String tableName) {
        return "SELECT gp_segment_id, count(*) FROM " + tableName + " GROUP BY gp_segment_id";
    }

    /**
     * 构建查询表数据倾斜语句
     */
    public String buildGetTableSkew(String tableName) {
        return "SELECT gp_segment_id, count(*) as cnt, " +
                "round(100 * count(*) / sum(count(*)) over(), 2) as percentage " +
                "FROM " + tableName + " GROUP BY gp_segment_id ORDER BY cnt DESC";
    }

    /**
     * 构建查询数据库大小语句
     */
    public String buildGetDatabaseSize(String databaseName) {
        return "SELECT pg_database_size('" + databaseName + "')";
    }

    /**
     * 构建查询表大小语句
     */
    public String buildGetTableSize(String tableName) {
        return "SELECT pg_size_pretty(pg_total_relation_size('" + tableName + "'))";
    }

    /**
     * 构建清理表语句（回收空间）
     */
    public String buildVacuumTable(String tableName) {
        return "VACUUM " + tableName;
    }

    /**
     * 构建分析表语句（更新统计信息）
     */
    public String buildAnalyzeTable(JQuickTableDefinition table, String tableName) {
        return "ANALYZE " + quoteIdentifier(table, tableName);
    }

    /**
     * 构建重分析表语句
     */
    public String buildReindexTable(String tableName) {
        return "REINDEX TABLE " + tableName;
    }

    /**
     * 构建刷新外部表语句
     */
    public String buildRefreshExternalTable(String tableName) {
        return "ALTER EXTERNAL TABLE " + tableName + " REFRESH";
    }

    /**
     * 构建外部表重新加载语句
     */
    public String buildReloadExternalTable(String tableName) {
        return "ALTER EXTERNAL TABLE " + tableName + " RELOAD";
    }
}
