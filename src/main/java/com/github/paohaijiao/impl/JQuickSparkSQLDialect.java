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
import com.github.paohaijiao.dataType.JQuickDataTypeConverter;
import com.github.paohaijiao.dataType.impl.JQuickSparkSQLDataTypeConverter;
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
 * Apache Spark SQL 方言实现
 * 支持 Spark SQL 的 DataFrame SQL 语法特性
 * <p>
 * Spark SQL 版本兼容性：
 * - Spark 2.x
 * - Spark 3.x
 * <p>
 * Spark SQL 与传统数据库的差异：
 * - 支持多种数据源（Hive, Parquet, ORC, JSON, JDBC 等）
 * - 支持 DataFrame API 和 SQL
 * - 支持流式处理
 * - 支持 Delta Lake
 * - 支持分区表和分桶表
 * - 查询引擎基于 Catalyst 优化器
 * - 适合大数据批处理和交互式查询
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/15
 */
public class JQuickSparkSQLDialect extends JQuickAbsSQLDialect {

    protected static final String SPARK_QUOTE = "`";

    @Override
    protected JQuickDataTypeConverter createDataTypeConvert() {
        return new JQuickSparkSQLDataTypeConverter();
    }

    @Override
    protected String getQuoteKeyWord() {
        return SPARK_QUOTE;
    }

    @Override
    public String getAutoIncrementKeyword(JQuickTableDefinition tableDefinition) {
        // Spark SQL 不支持自增列
        return "";
    }

    @Override
    protected void appendTableOptions(StringBuilder sql, JQuickTableDefinition table) {
        // Spark SQL 表选项
        if (table.getExtensions() != null) {
            // 数据源格式
            if (table.getExtensions().containsKey("using")) {
                String using = table.getExtensions().get("using").toString();
                sql.append("\nUSING ").append(using.toUpperCase());
            } else {
                // 默认使用 Parquet
                sql.append("\nUSING PARQUET");
            }
            // 分区列
            if (table.getExtensions().containsKey("partitionedBy")) {
                @SuppressWarnings("unchecked")
                List<String> partitionColumns = (List<String>) table.getExtensions().get("partitionedBy");
                if (partitionColumns != null && !partitionColumns.isEmpty()) {
                    sql.append("\nPARTITIONED BY (");
                    sql.append(partitionColumns.stream()
                            .map(c -> quoteIdentifier(table, c))
                            .collect(Collectors.joining(", ")));
                    sql.append(")");
                }
            }
            // 分桶列
            if (table.getExtensions().containsKey("bucketedBy")) {
                @SuppressWarnings("unchecked")
                List<String> bucketColumns = (List<String>) table.getExtensions().get("bucketedBy");
                if (bucketColumns != null && !bucketColumns.isEmpty()) {
                    sql.append("\nCLUSTERED BY (");
                    sql.append(bucketColumns.stream()
                            .map(c -> quoteIdentifier(table, c))
                            .collect(Collectors.joining(", ")));
                    sql.append(")");

                    // 分桶数
                    if (table.getExtensions().containsKey("bucketCount")) {
                        Integer bucketCount = (Integer) table.getExtensions().get("bucketCount");
                        if (bucketCount != null && bucketCount > 0) {
                            sql.append("\nINTO ").append(bucketCount).append(" BUCKETS");
                        }
                    }
                }
            }

            // 排序分桶
            if (table.getExtensions().containsKey("sortedBy")) {
                @SuppressWarnings("unchecked")
                List<String> sortedColumns = (List<String>) table.getExtensions().get("sortedBy");
                if (sortedColumns != null && !sortedColumns.isEmpty()) {
                    sql.append("\nSORTED BY (");
                    sql.append(sortedColumns.stream()
                            .map(c -> quoteIdentifier(table, c))
                            .collect(Collectors.joining(", ")));
                    sql.append(")");
                }
            }

            // 位置
            if (table.getExtensions().containsKey("location")) {
                String location = table.getExtensions().get("location").toString();
                sql.append("\nLOCATION '").append(escapeString(location)).append("'");
            }

            // 表注释
            if (table.getComment() != null && !table.getComment().isEmpty()) {
                sql.append("\nCOMMENT '").append(escapeString(table.getComment())).append("'");
            }

            // TBLPROPERTIES
            if (table.getExtensions().containsKey("tblProperties")) {
                @SuppressWarnings("unchecked")
                Map<String, String> tblProps = (Map<String, String>) table.getExtensions().get("tblProperties");
                if (tblProps != null && !tblProps.isEmpty()) {
                    sql.append("\nTBLPROPERTIES (");
                    boolean first = true;
                    for (Map.Entry<String, String> entry : tblProps.entrySet()) {
                        if (!first) {
                            sql.append(", ");
                        }
                        sql.append("'").append(escapeString(entry.getKey())).append("'='")
                                .append(escapeString(entry.getValue())).append("'");
                        first = false;
                    }
                    sql.append(")");
                }
            }

            // 压缩选项
            if (table.getExtensions().containsKey("compression")) {
                String compression = table.getExtensions().get("compression").toString();
                sql.append("\nTBLPROPERTIES ('parquet.compression'='").append(compression.toLowerCase()).append("')");
            }

            // Delta Lake 表选项
            if (table.getExtensions().containsKey("deltaTable")) {
                Boolean deltaTable = (Boolean) table.getExtensions().get("deltaTable");
                if (deltaTable != null && deltaTable) {
                    sql.append("\nUSING DELTA");
                }
            }
        }
    }

    @Override
    public String getDriverClass(JQuickDataSourceConnector connector) {
        if (connector != null && connector.getDriverClass() != null && !connector.getDriverClass().trim().isEmpty()) {
            return connector.getDriverClass();
        }
        return "org.apache.hive.jdbc.HiveDriver";
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
            throw new IllegalStateException("Host is required for Spark SQL connection");
        }
        String effectivePort = (port != null && !port.trim().isEmpty()) ? port : "10015";
        String effectiveDatabase = (database != null && !database.trim().isEmpty()) ? database : "default";
        StringBuilder url = new StringBuilder();
        url.append("jdbc:hive2://").append(host).append(":").append(effectivePort);
        url.append("/").append(effectiveDatabase);
        boolean hasParams = false;
        if (username != null && !username.trim().isEmpty()) {
            url.append(";user=").append(username);
            hasParams = true;
        }
        if (password != null && !password.trim().isEmpty()) {
            url.append(";password=").append(password);
            hasParams = true;
        }
        String authMode = connector.getByKeyStr("authMode");
        if (authMode != null && !authMode.isEmpty()) {
            url.append(hasParams ? ";" : "").append("auth=").append(authMode);
            hasParams = true;
        }

        return url.toString();
    }

    @Override
    public String buildCreateTable(JQuickTableDefinition table) {
        StringBuilder sql = new StringBuilder();
        // CREATE TABLE 语句
        sql.append("CREATE TABLE ");
        // IF NOT EXISTS
        if (table.getExtensions() != null && table.getExtensions().containsKey("ifNotExists")) {
            Boolean ifNotExists = (Boolean) table.getExtensions().get("ifNotExists");
            if (ifNotExists != null && ifNotExists) {
                sql.append("IF NOT EXISTS ");
            }
        }
        sql.append(quoteIdentifier(table, table.getTableName()));
        // 列定义
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
            if (i < columns.size() - 1) {
                sql.append(",");
            }
            sql.append(NEW_LINE);
        }
        sql.append(")");
        appendTableOptions(sql, table);
        return sql.toString();
    }

    /**
     * 构建创建表 AS SELECT 语句
     */
    public String buildCreateTableAsSelect(JQuickTableDefinition table, String query) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE ").append(quoteIdentifier(table, table.getTableName()));
        if (table.getExtensions() != null && table.getExtensions().containsKey("ifNotExists")) {
            Boolean ifNotExists = (Boolean) table.getExtensions().get("ifNotExists");
            if (ifNotExists != null && ifNotExists) {
                sql.append(" IF NOT EXISTS");
            }
        }
        appendTableOptions(sql, table);
        sql.append(" AS\n").append(query);
        return sql.toString();
    }

    /**
     * 构建创建临时视图语句
     */
    public String buildCreateTempView(String viewName, String query) {
        return "CREATE OR REPLACE TEMP VIEW " + viewName + " AS\n" + query;
    }

    /**
     * 构建创建全局临时视图语句
     */
    public String buildCreateGlobalTempView(String viewName, String query) {
        return "CREATE OR REPLACE GLOBAL TEMP VIEW " + viewName + " AS\n" + query;
    }

    @Override
    public String buildColumnDefinition(JQuickTableDefinition table, JQuickColumnDefinition column) {
        StringBuilder def = new StringBuilder();
        def.append(quoteIdentifier(table, column.getColumnName())).append(SPACE);
        def.append(getDataTypeString(table, column.getDataType()));
        if (column.getComment() != null && !column.getComment().isEmpty()) {
            def.append(" COMMENT '").append(escapeString(column.getComment())).append("'");
        }
        return def.toString();
    }

    @Override
    protected void appendNullClause(StringBuilder def, JQuickColumnDefinition column) {
        // Spark SQL 支持 NOT NULL
        if (!column.isNullable()) {
            def.append(" NOT NULL");
        }
    }

    @Override
    protected void appendDefaultClause(StringBuilder def, JQuickColumnDefinition column) {
        // Spark SQL 不支持 DEFAULT 值
    }

    @Override
    protected void appendColumnComment(StringBuilder def, JQuickColumnDefinition column) {
        // 已在 buildColumnDefinition 中处理
    }

    @Override
    protected String getSpecialDefaultKeyword(String value) {
        // Spark SQL 不支持 DEFAULT
        return null;
    }

    @Override
    protected String formatBooleanDefault(boolean value) {
        return value ? "true" : "false";
    }

    @Override
    protected String convertForeignKeyAction(JQuickForeignKeyAction action) {
        // Spark SQL 不支持外键约束
        return "";
    }

    @Override
    public String buildPrimaryKey(JQuickTableDefinition table, com.github.paohaijiao.extra.JQuickPrimaryKeyConstraint pk) {
        // Spark SQL 对主键支持有限
        if (table.getExtensions() != null && "delta".equalsIgnoreCase(getStorageFormat(table))) {
            String sb = "CONSTRAINT " + quoteIdentifier(table, pk.getConstraintName()) +
                    " PRIMARY KEY (" +
                    formatColumnList(table, pk.getColumns()) +
                    ") NOT ENFORCED";
            return sb;
        }
        return "";
    }

    @Override
    public String buildUniqueConstraint(JQuickTableDefinition table, com.github.paohaijiao.extra.JQuickUniqueConstraint uc) {
        // Spark SQL 不支持唯一约束
        return "";
    }

    @Override
    public String buildForeignKey(JQuickTableDefinition table, JQuickForeignKeyConstraint fk) {
        // Spark SQL 不支持外键约束
        return "";
    }

    @Override
    public String buildModifyColumn(JQuickTableDefinition table, String tableName, JQuickColumnDefinition column) {
        // Spark SQL 支持修改列
        String sb = "ALTER TABLE " + quoteIdentifier(table, tableName) + " ALTER COLUMN " + quoteIdentifier(table, column.getColumnName()) + " TYPE " + getDataTypeString(table, column.getDataType());
        return sb;
    }

    @Override
    public String buildChangeColumn(JQuickTableDefinition table, String tableName,
                                    String oldName, JQuickColumnDefinition newColumn) {
        StringBuilder sb = new StringBuilder();
        // Spark SQL 支持重命名列
        if (!oldName.equals(newColumn.getColumnName())) {
            sb.append("ALTER TABLE ").append(quoteIdentifier(table, tableName));
            sb.append(" RENAME COLUMN ").append(quoteIdentifier(table, oldName));
            sb.append(" TO ").append(quoteIdentifier(table, newColumn.getColumnName()));
            sb.append(";\n");
        }
        // 修改数据类型
        sb.append("ALTER TABLE ").append(quoteIdentifier(table, tableName));
        sb.append(" ALTER COLUMN ").append(quoteIdentifier(table, newColumn.getColumnName()));
        sb.append(" TYPE ").append(getDataTypeString(table, newColumn.getDataType()));

        return sb.toString();
    }

    @Override
    public String buildShowCreateTable(JQuickTableDefinition table, String tableName) {
        return "SHOW CREATE TABLE " + quoteIdentifier(table, tableName);
    }

    @Override
    public String buildDescribeTable(JQuickTableDefinition table, String tableName) {
        return "DESCRIBE EXTENDED " + quoteIdentifier(table, tableName);
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
        sb.append(" (").append(columns.stream().map(e -> quoteIdentifier(table, e)).collect(Collectors.joining(", ")));
        sb.append(") VALUES ");
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

    /**
     * 构建从查询插入的语句
     */
    public String buildInsertFromQuery(JQuickTableDefinition table, String query) {
        return "INSERT INTO " + quoteIdentifier(table, table.getTableName()) + "\n" + query;
    }

    /**
     * 构建覆盖写入语句
     */
    public String buildInsertOverwrite(JQuickTableDefinition table, String query) {
        return "INSERT OVERWRITE " + quoteIdentifier(table, table.getTableName()) + "\n" + query;
    }

    /**
     * 构建动态分区插入语句
     */
    public String buildInsertDynamicPartition(JQuickTableDefinition table, String query, List<String> partitionColumns) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT OVERWRITE ").append(quoteIdentifier(table, table.getTableName()));
        if (partitionColumns != null && !partitionColumns.isEmpty()) {
            sb.append(" PARTITION (");
            sb.append(partitionColumns.stream()
                    .map(c -> quoteIdentifier(table, c))
                    .collect(Collectors.joining(", ")));
            sb.append(")");
        }
        sb.append("\n").append(query);
        return sb.toString();
    }

    /**
     * 构建追加写入语句
     */
    public String buildInsertAppend(JQuickTableDefinition table, String query) {
        return "INSERT INTO " + quoteIdentifier(table, table.getTableName()) + "\n" + query;
    }

    @Override
    public String buildUpdate(JQuickTableDefinition table, JQuickRow row, String whereClause) {
        // Spark SQL 支持 UPDATE（Delta Lake）
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

    /**
     * 构建合并语句（Merge into）
     */
    public String buildMergeInto(JQuickTableDefinition targetTable, String sourceTable, String condition, List<Map<String, Object>> actions) {
        StringBuilder sb = new StringBuilder();
        sb.append("MERGE INTO ").append(quoteIdentifier(targetTable, targetTable.getTableName()));
        sb.append(" USING ").append(sourceTable);
        sb.append(" ON ").append(condition);
        for (Map<String, Object> action : actions) {
            String type = (String) action.get("type");
            if ("WHEN_MATCHED".equalsIgnoreCase(type)) {
                sb.append("\nWHEN MATCHED THEN ");
                if (action.containsKey("update")) {
                    @SuppressWarnings("unchecked")
                    Map<String, String> updates = (Map<String, String>) action.get("update");
                    sb.append("UPDATE SET ");
                    boolean first = true;
                    for (Map.Entry<String, String> entry : updates.entrySet()) {
                        if (!first) {
                            sb.append(", ");
                        }
                        sb.append(quoteIdentifier(targetTable, entry.getKey()))
                                .append(" = ").append(entry.getValue());
                        first = false;
                    }
                } else if (action.containsKey("delete")) {
                    sb.append("DELETE");
                }
            } else if ("WHEN_NOT_MATCHED".equalsIgnoreCase(type)) {
                sb.append("\nWHEN NOT MATCHED THEN ");
                if (action.containsKey("insert")) {
                    @SuppressWarnings("unchecked")
                    Map<String, String> inserts = (Map<String, String>) action.get("insert");
                    sb.append("INSERT (");
                    sb.append(inserts.keySet().stream()
                            .map(c -> quoteIdentifier(targetTable, c))
                            .collect(Collectors.joining(", ")));
                    sb.append(") VALUES (");
                    sb.append(inserts.values().stream().collect(Collectors.joining(", ")));
                    sb.append(")");
                }
            }
        }
        return sb.toString();
    }

    @Override
    public String buildDelete(JQuickTableDefinition table, String whereClause) {
        // Spark SQL 支持 DELETE（Delta Lake）
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
        // DISTINCT
        if (table.getExtensions() != null && table.getExtensions().containsKey("distinct")) {
            Boolean distinct = (Boolean) table.getExtensions().get("distinct");
            if (distinct != null && distinct) {
                sb.append("DISTINCT ");
            }
        }
        // 提示
        if (table.getExtensions() != null && table.getExtensions().containsKey("hint")) {
            String hint = table.getExtensions().get("hint").toString();
            sb.append("/*+ ").append(hint).append(" */ ");
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
        // Spark SQL 不支持索引
        return "";
    }

    /**
     * 获取存储格式
     */
    private String getStorageFormat(JQuickTableDefinition table) {
        if (table.getExtensions() != null && table.getExtensions().containsKey("using")) {
            return table.getExtensions().get("using").toString().toLowerCase();
        }
        return "parquet";
    }

    /**
     * 构建添加分区语句
     */
    public String buildAddPartition(JQuickTableDefinition table, String tableName, Map<String, String> partition) {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ").append(quoteIdentifier(table, tableName));
        sb.append(" ADD ");
        if (partition != null && !partition.isEmpty()) {
            sb.append("PARTITION (");
            boolean first = true;
            for (Map.Entry<String, String> entry : partition.entrySet()) {
                if (!first) {
                    sb.append(", ");
                }
                sb.append(quoteIdentifier(table, entry.getKey())).append(" = '")
                        .append(escapeString(entry.getValue())).append("'");
                first = false;
            }
            sb.append(")");
        }
        if (table.getExtensions() != null && table.getExtensions().containsKey("partitionLocation")) {
            String location = table.getExtensions().get("partitionLocation").toString();
            sb.append(" LOCATION '").append(escapeString(location)).append("'");
        }
        return sb.toString();
    }

    /**
     * 构建删除分区语句
     */
    public String buildDropPartition(JQuickTableDefinition table, String tableName, Map<String, String> partition) {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ").append(quoteIdentifier(table, tableName));
        sb.append(" DROP ");
        if (partition != null && !partition.isEmpty()) {
            sb.append("PARTITION (");
            boolean first = true;
            for (Map.Entry<String, String> entry : partition.entrySet()) {
                if (!first) {
                    sb.append(", ");
                }
                sb.append(quoteIdentifier(table, entry.getKey())).append(" = '")
                        .append(escapeString(entry.getValue())).append("'");
                first = false;
            }
            sb.append(")");
        }

        return sb.toString();
    }

    /**
     * 构建显示分区语句
     */
    public String buildShowPartitions(JQuickTableDefinition table, String tableName) {
        return "SHOW PARTITIONS " + quoteIdentifier(table, tableName);
    }

    /**
     * 构建修复分区语句（MSCK）
     */
    public String buildRepairPartitions(JQuickTableDefinition table, String tableName) {
        return "MSCK REPAIR TABLE " + quoteIdentifier(table, tableName);
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
        // 删除数据
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
     * 构建缓存表语句
     */
    public String buildCacheTable(String tableName) {
        return "CACHE TABLE " + tableName;
    }

    /**
     * 构建缓存表语句（带惰性加载）
     */
    public String buildCacheTableLazy(String tableName) {
        return "CACHE LAZY TABLE " + tableName;
    }

    /**
     * 构建清除缓存语句
     */
    public String buildUncacheTable(String tableName) {
        return "UNCACHE TABLE " + tableName;
    }

    /**
     * 构建清除缓存语句（带是否存在检查）
     */
    public String buildUncacheTableIfExists(String tableName) {
        return "UNCACHE TABLE IF EXISTS " + tableName;
    }

    /**
     * 构建刷新表语句
     */
    public String buildRefreshTable(String tableName) {
        return "REFRESH TABLE " + tableName;
    }
    /**
     * 构建创建视图语句
     */
    public String buildCreateView(String viewName, String query) {
        return "CREATE VIEW " + viewName + " AS\n" + query;
    }

    /**
     * 构建创建视图语句（带替换）
     */
    public String buildCreateOrReplaceView(String viewName, String query) {
        return "CREATE OR REPLACE VIEW " + viewName + " AS\n" + query;
    }

    /**
     * 构建创建全局视图语句
     */
    public String buildCreateGlobalView(String viewName, String query) {
        return "CREATE GLOBAL TEMP VIEW " + viewName + " AS\n" + query;
    }

    /**
     * 构建分析表语句（计算统计信息）
     */
    public String buildAnalyzeTable(JQuickTableDefinition table, String tableName) {
        return "ANALYZE TABLE " + quoteIdentifier(table, tableName) + " COMPUTE STATISTICS";
    }

    /**
     * 构建分析表语句（指定列）
     */
    public String buildAnalyzeTableColumns(JQuickTableDefinition table, String tableName, List<String> columns) {
        String sb = "ANALYZE TABLE " + quoteIdentifier(table, tableName) +
                " COMPUTE STATISTICS FOR COLUMNS " +
                columns.stream().map(c -> quoteIdentifier(table, c)).collect(Collectors.joining(", "));
        return sb;
    }

    /**
     * 构建分析分区语句
     */
    public String buildAnalyzePartition(JQuickTableDefinition table, String tableName, Map<String, String> partition) {
        StringBuilder sb = new StringBuilder();
        sb.append("ANALYZE TABLE ").append(quoteIdentifier(table, tableName));
        if (partition != null && !partition.isEmpty()) {
            sb.append(" PARTITION (");
            boolean first = true;
            for (Map.Entry<String, String> entry : partition.entrySet()) {
                if (!first) {
                    sb.append(", ");
                }
                sb.append(quoteIdentifier(table, entry.getKey())).append(" = '")
                        .append(escapeString(entry.getValue())).append("'");
                first = false;
            }
            sb.append(")");
        }
        sb.append(" COMPUTE STATISTICS");
        return sb.toString();
    }

    /**
     * 构建显示统计信息语句
     */
    public String buildShowTableStats(JQuickTableDefinition table, String tableName) {
        return "DESCRIBE EXTENDED " + quoteIdentifier(table, tableName);
    }
    /**
     * 构建分页查询语句（使用 LIMIT 和 OFFSET）
     */
    public String buildPaginationQuery(JQuickTableDefinition table, List<String> columns, String whereClause, String orderBy, int offset, int limit) {
        StringBuilder sql = new StringBuilder();
        sql.append(buildSelect(table, columns, whereClause));
        if (orderBy != null && !orderBy.isEmpty()) {
            sql.append(" ORDER BY ").append(orderBy);
        }
        sql.append(" LIMIT ").append(limit);
        if (offset > 0) {
            sql.append(" OFFSET ").append(offset);
        }
        return sql.toString();
    }
    /**
     * 构建创建数据库语句
     */
    public String buildCreateDatabase(String databaseName) {
        return "CREATE DATABASE IF NOT EXISTS " + databaseName;
    }

    /**
     * 构建删除数据库语句
     */
    public String buildDropDatabase(String databaseName, boolean cascade) {
        StringBuilder sb = new StringBuilder("DROP DATABASE IF EXISTS ");
        sb.append(databaseName);
        if (cascade) {
            sb.append(" CASCADE");
        }
        return sb.toString();
    }

    /**
     * 构建使用数据库语句
     */
    public String buildUseDatabase(String databaseName) {
        return "USE " + databaseName;
    }

    /**
     * 构建显示数据库语句
     */
    public String buildShowDatabases() {
        return "SHOW DATABASES";
    }

    /**
     * 构建显示表语句
     */
    public String buildShowTables() {
        return "SHOW TABLES";
    }

    /**
     * 构建显示函数语句
     */
    public String buildShowFunctions() {
        return "SHOW FUNCTIONS";
    }

    /**
     * 构建设置 Spark 参数语句
     */
    public String buildSetParameter(String key, String value) {
        return "SET " + key + " = " + value;
    }

    /**
     * 构建重置参数语句
     */
    public String buildResetParameter(String key) {
        return "RESET " + key;
    }

    /**
     * 构建设置并行度语句
     */
    public String buildSetParallelism(int parallelism) {
        return "SET spark.sql.shuffle.partitions = " + parallelism;
    }

    /**
     * 构建启用自适应查询执行
     */
    public String buildEnableAdaptiveQueryExecution(boolean enable) {
        return "SET spark.sql.adaptive.enabled = " + enable;
    }

    /**
     * 构建启用动态分区插入
     */
    public String buildEnableDynamicPartition(boolean enable) {
        if (enable) {
            return "SET spark.sql.sources.partitionOverwriteMode = dynamic";
        }
        return "SET spark.sql.sources.partitionOverwriteMode = static";
    }

    /**
     * 构建广播提示
     */
    public String buildBroadcastHint(String tableName) {
        return "/*+ BROADCAST(" + tableName + ") */";
    }

    /**
     * 构建合并提示
     */
    public String buildMergeHint(String tableName) {
        return "/*+ MERGE(" + tableName + ") */";
    }

    /**
     * 构建排序合并提示
     */
    public String buildSortMergeHint(String tableName) {
        return "/*+ SORTMERGE(" + tableName + ") */";
    }

    /**
     * 构建洗牌哈希提示
     */
    public String buildShuffleHashHint(String tableName) {
        return "/*+ SHUFFLE_HASH(" + tableName + ") */";
    }

    /**
     * 构建时间旅行查询（按版本）
     */
    public String buildTimeTravelByVersion(String tableName, long version) {
        return "SELECT * FROM " + tableName + " VERSION AS OF " + version;
    }

    /**
     * 构建时间旅行查询（按时间戳）
     */
    public String buildTimeTravelByTimestamp(String tableName, String timestamp) {
        return "SELECT * FROM " + tableName + " TIMESTAMP AS OF '" + timestamp + "'";
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
    public String buildExplainExtended(String query) {
        return "EXPLAIN EXTENDED " + query;
    }

    /**
     * 构建格式化查询计划语句
     */
    public String buildExplainFormatted(String query) {
        return "EXPLAIN FORMATTED " + query;
    }

    /**
     * 构建代码生成查询计划语句
     */
    public String buildExplainCodegen(String query) {
        return "EXPLAIN CODEGEN " + query;
    }

    /**
     * 构建成本查询计划语句
     */
    public String buildExplainCost(String query) {
        return "EXPLAIN COST " + query;
    }
    /**
     * 构建导出数据语句
     */
    public String buildExportData(String tableName, String format, String path) {
        return "INSERT OVERWRITE DIRECTORY '" + path + "'\n" + "USING " + format + "\n" + "SELECT * FROM " + tableName;
    }

    /**
     * 构建导出数据语句（带分区）
     */
    public String buildExportDataPartitioned(String tableName, String format, String path, Map<String, String> partition) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT OVERWRITE DIRECTORY '").append(path).append("'\n");
        sb.append("USING ").append(format).append("\n");
        if (partition != null && !partition.isEmpty()) {
            sb.append("PARTITIONED BY (");
            boolean first = true;
            for (Map.Entry<String, String> entry : partition.entrySet()) {
                if (!first) {
                    sb.append(", ");
                }
                sb.append(entry.getKey());
                first = false;
            }
            sb.append(")\n");
        }
        sb.append("SELECT * FROM ").append(tableName);
        return sb.toString();
    }
}
