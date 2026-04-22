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
import com.github.paohaijiao.dataType.impl.JQuickPrestoDataTypeConverter;
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
 * Presto/Trino 方言实现
 * 支持 Presto 和 Trino 分布式 SQL 查询引擎的语法特性
 *
 * Presto/Trino 版本兼容性：
 * - Presto 0.1xx - 0.2xx
 * - Trino 3xx - 4xx
 *
 * Presto/Trino 与传统数据库的差异：
 * - 主要用于查询，对 DDL 支持有限
 * - 不支持行级 INSERT/UPDATE/DELETE
 * - 不支持事务
 * - 不支持自增列
 * - 不支持外键约束
 * - 不支持索引
 * - 支持连接多种数据源（Hive, MySQL, PostgreSQL, Kafka 等）
 * - 查询性能极高，适合联邦查询
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/15
 */
public class JQuickPrestoDialect extends JQuickAbsSQLDialect {

    protected static final String PRESTO_QUOTE = "\"";

    @Override
    protected JQuickDataTypeConverter createDataTypeConvert() {
        return new JQuickPrestoDataTypeConverter();
    }

    @Override
    protected String getQuoteKeyWord() {
        return PRESTO_QUOTE;
    }

    @Override
    public String getAutoIncrementKeyword(JQuickTableDefinition tableDefinition) {
        // Presto/Trino 不支持自增列
        return "";
    }

    @Override
    protected void appendTableOptions(StringBuilder sql, JQuickTableDefinition table) {
        // Presto/Trino 表选项
        if (table.getExtensions() != null) {
            // 存储格式
            if (table.getExtensions().containsKey("format")) {
                String format = table.getExtensions().get("format").toString();
                sql.append("\nWITH (format = '").append(format.toUpperCase()).append("')");
            }
            // 分区列
            if (table.getExtensions().containsKey("partitionedBy")) {
                @SuppressWarnings("unchecked")
                List<String> partitionColumns = (List<String>) table.getExtensions().get("partitionedBy");
                if (partitionColumns != null && !partitionColumns.isEmpty()) {
                    String partitionStr = partitionColumns.stream()
                            .map(c -> "'" + c + "'")
                            .collect(Collectors.joining(", "));
                    sql.append(", partitioned_by = ARRAY[").append(partitionStr).append("]");
                }
            }
            // 分桶列
            if (table.getExtensions().containsKey("bucketedBy")) {
                @SuppressWarnings("unchecked")
                List<String> bucketColumns = (List<String>) table.getExtensions().get("bucketedBy");
                if (bucketColumns != null && !bucketColumns.isEmpty()) {
                    String bucketStr = bucketColumns.stream()
                            .map(c -> "'" + c + "'")
                            .collect(Collectors.joining(", "));
                    sql.append(", bucketed_by = ARRAY[").append(bucketStr).append("]");
                }
                // 分桶数
                if (table.getExtensions().containsKey("bucketCount")) {
                    Integer bucketCount = (Integer) table.getExtensions().get("bucketCount");
                    if (bucketCount != null && bucketCount > 0) {
                        sql.append(", bucket_count = ").append(bucketCount);
                    }
                }
            }
            // 排序列
            if (table.getExtensions().containsKey("sortedBy")) {
                @SuppressWarnings("unchecked")
                List<String> sortedColumns = (List<String>) table.getExtensions().get("sortedBy");
                if (sortedColumns != null && !sortedColumns.isEmpty()) {
                    String sortedStr = sortedColumns.stream()
                            .map(c -> "'" + c + "'")
                            .collect(Collectors.joining(", "));
                    sql.append(", sorted_by = ARRAY[").append(sortedStr).append("]");
                }
            }
            // 位置（HDFS 路径）
            if (table.getExtensions().containsKey("location")) {
                String location = table.getExtensions().get("location").toString();
                sql.append(", location = '").append(escapeString(location)).append("'");
            }
            // 表注释
            if (table.getComment() != null && !table.getComment().isEmpty()) {
                sql.append(", comment = '").append(escapeString(table.getComment())).append("'");
            }
            // ORC 格式选项
            if (table.getExtensions().containsKey("orcCompression")) {
                String compression = table.getExtensions().get("orcCompression").toString();
                sql.append(", orc_compression = '").append(compression.toUpperCase()).append("'");
            }
            // Parquet 格式选项
            if (table.getExtensions().containsKey("parquetCompression")) {
                String compression = table.getExtensions().get("parquetCompression").toString();
                sql.append(", parquet_compression = '").append(compression.toUpperCase()).append("'");
            }
        }
    }

    @Override
    public String getDriverClass(JQuickDataSourceConnector connector) {
        if (connector != null && connector.getDriverClass() != null && !connector.getDriverClass().isEmpty()) {
            return connector.getDriverClass();
        }
        String mode = connector != null ? connector.getByKeyStr("mode") : null;
        if ("postgresql".equalsIgnoreCase(mode)) {
            return "org.postgresql.Driver";
        }
        return "com.mysql.cj.jdbc.Driver";
    }

    @Override
    public String getUrl(JQuickDataSourceConnector connector) {
        if (connector == null) {
            throw new IllegalArgumentException("Connector cannot be null");
        }
        if (connector.getUrl() != null && !connector.getUrl().isEmpty()) {
            return connector.getUrl();
        }
        String host = connector.getHost();
        String port = connector.getPort();
        String database = connector.getSchema();
        String username = connector.getUsername();
        String password = connector.getPassword();
        if (host == null || host.isEmpty()) {
            throw new IllegalStateException("Host is required for PolarDB");
        }
        String mode = connector.getByKeyStr("mode");
        boolean isPostgresqlMode = "postgresql".equalsIgnoreCase(mode);
        String effectivePort = (port != null && !port.isEmpty()) ? port : (isPostgresqlMode ? "5432" : "3306");
        String effectiveDatabase = (database != null && !database.isEmpty()) ? database : "";
        StringBuilder url = new StringBuilder();
        if (isPostgresqlMode) {
            url.append("jdbc:postgresql://").append(host).append(":").append(effectivePort);
            url.append("/").append(effectiveDatabase);
        } else {
            url.append("jdbc:mysql://").append(host).append(":").append(effectivePort);
            url.append("/").append(effectiveDatabase);
        }
        boolean hasParams = false;
        if (username != null && !username.isEmpty()) {
            url.append("?user=").append(username);
            hasParams = true;
        }
        if (password != null && !password.isEmpty()) {
            url.append(hasParams ? "&" : "?").append("password=").append(password);
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
        // Presto/Trino 在 CREATE TABLE 中不支持 NOT NULL
    }

    @Override
    protected void appendDefaultClause(StringBuilder def, JQuickColumnDefinition column) {
        // Presto/Trino 不支持 DEFAULT
    }

    @Override
    protected void appendColumnComment(StringBuilder def, JQuickColumnDefinition column) {
        // 已在 buildColumnDefinition 中处理
    }

    @Override
    protected String getSpecialDefaultKeyword(String value) {
        // Presto/Trino 不支持 DEFAULT
        return null;
    }

    @Override
    protected String formatBooleanDefault(boolean value) {
        return value ? "TRUE" : "FALSE";
    }

    @Override
    protected String convertForeignKeyAction(JQuickForeignKeyAction action) {
        // Presto/Trino 不支持外键约束
        return "";
    }

    @Override
    public String buildPrimaryKey(JQuickTableDefinition table, com.github.paohaijiao.extra.JQuickPrimaryKeyConstraint pk) {
        // Presto/Trino 不支持主键约束
        return "";
    }

    @Override
    public String buildUniqueConstraint(JQuickTableDefinition table, com.github.paohaijiao.extra.JQuickUniqueConstraint uc) {
        // Presto/Trino 不支持唯一约束
        return "";
    }

    @Override
    public String buildForeignKey(JQuickTableDefinition table, JQuickForeignKeyConstraint fk) {
        // Presto/Trino 不支持外键约束
        return "";
    }

    @Override
    public String buildModifyColumn(JQuickTableDefinition table, String tableName, JQuickColumnDefinition column) {
        // Presto/Trino 支持修改列
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ").append(quoteIdentifier(table, tableName));
        sb.append(" ALTER COLUMN ").append(quoteIdentifier(table, column.getColumnName()));
        sb.append(" SET DATA TYPE ").append(getDataTypeString(table, column.getDataType()));

        return sb.toString();
    }

    @Override
    public String buildChangeColumn(JQuickTableDefinition table, String tableName, String oldName, JQuickColumnDefinition newColumn) {
        StringBuilder sb = new StringBuilder();
        // Presto/Trino 支持重命名列
        if (!oldName.equals(newColumn.getColumnName())) {
            sb.append("ALTER TABLE ").append(quoteIdentifier(table, tableName));
            sb.append(" RENAME COLUMN ").append(quoteIdentifier(table, oldName));
            sb.append(" TO ").append(quoteIdentifier(table, newColumn.getColumnName()));
            sb.append(";\n");
        }
        // 修改数据类型
        sb.append("ALTER TABLE ").append(quoteIdentifier(table, tableName));
        sb.append(" ALTER COLUMN ").append(quoteIdentifier(table, newColumn.getColumnName()));
        sb.append(" SET DATA TYPE ").append(getDataTypeString(table, newColumn.getDataType()));
        return sb.toString();
    }

    @Override
    public String buildShowCreateTable(JQuickTableDefinition table, String tableName) {
        return "SHOW CREATE TABLE " + quoteIdentifier(table, tableName);
    }

    @Override
    public String buildDescribeTable(JQuickTableDefinition table, String tableName) {
        return "DESCRIBE " + quoteIdentifier(table, tableName);
    }

    @Override
    public String buildInsert(JQuickTableDefinition table, JQuickRow row) {
        // Presto/Trino 支持 INSERT，但不推荐单行插入
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
        // 获取所有列名
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

    @Override
    public String buildUpdate(JQuickTableDefinition table, JQuickRow row, String whereClause) {
        // Presto/Trino 支持 UPDATE（某些连接器）
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
        // Presto/Trino 支持 DELETE（某些连接器）
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
        // APPROXIMATE DISTINCT
        if (table.getExtensions() != null && table.getExtensions().containsKey("approxDistinct")) {
            Boolean approx = (Boolean) table.getExtensions().get("approxDistinct");
            if (approx != null && approx) {
                sb.append("APPROXIMATE DISTINCT ");
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
        // Presto/Trino 不支持索引
        return "";
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
        return "SHOW PARTITIONS FROM " + quoteIdentifier(table, tableName);
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
     * 构建创建视图语句
     */
    public String buildCreateView(String viewName, String query) {
        return "CREATE VIEW " + viewName + " AS\n" + query;
    }

    /**
     * 构建创建物化视图语句
     */
    public String buildCreateMaterializedView(String viewName, String query) {
        return "CREATE MATERIALIZED VIEW " + viewName + " AS\n" + query;
    }

    /**
     * 构建刷新物化视图语句
     */
    public String buildRefreshMaterializedView(String viewName) {
        return "REFRESH MATERIALIZED VIEW " + viewName;
    }

    /**
     * 构建删除视图语句
     */
    public String buildDropView(String viewName, boolean ifExists) {
        StringBuilder sb = new StringBuilder("DROP VIEW ");
        if (ifExists) {
            sb.append("IF EXISTS ");
        }
        sb.append(viewName);
        return sb.toString();
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
        sql.append(" OFFSET ").append(offset).append(" ROWS");
        sql.append(" FETCH NEXT ").append(limit).append(" ROWS ONLY");
        return sql.toString();
    }

    /**
     * 构建简单 LIMIT 查询
     */
    public String buildLimitQuery(JQuickTableDefinition table, List<String> columns, String whereClause, String orderBy, int limit) {
        StringBuilder sql = new StringBuilder();
        sql.append(buildSelect(table, columns, whereClause));
        if (orderBy != null && !orderBy.isEmpty()) {
            sql.append(" ORDER BY ").append(orderBy);
        }
        sql.append(" LIMIT ").append(limit);
        return sql.toString();
    }

    /**
     * 构建分析表语句（计算统计信息）
     */
    public String buildAnalyzeTable(JQuickTableDefinition table, String tableName) {
        return "ANALYZE " + quoteIdentifier(table, tableName);
    }

    /**
     * 构建分析分区语句
     */
    public String buildAnalyzePartition(JQuickTableDefinition table, String tableName, Map<String, String> partition) {
        StringBuilder sb = new StringBuilder();
        sb.append("ANALYZE ").append(quoteIdentifier(table, tableName));
        if (partition != null && !partition.isEmpty()) {
            sb.append(" WITH (partitions = ARRAY[");
            sb.append("ROW(");
            boolean first = true;
            for (Map.Entry<String, String> entry : partition.entrySet()) {
                if (!first) {
                    sb.append(", ");
                }
                sb.append("'").append(escapeString(entry.getValue())).append("'");
                first = false;
            }
            sb.append(")");
            sb.append("])");
        }
        return sb.toString();
    }


    /**
     * 构建创建 Schema 语句
     */
    public String buildCreateSchema(String schemaName) {
        return "CREATE SCHEMA " + schemaName;
    }

    /**
     * 构建删除 Schema 语句
     */
    public String buildDropSchema(String schemaName, boolean ifExists) {
        StringBuilder sb = new StringBuilder("DROP SCHEMA ");
        if (ifExists) {
            sb.append("IF EXISTS ");
        }
        sb.append(schemaName);
        return sb.toString();
    }

    /**
     * 构建显示 Schema 语句
     */
    public String buildShowSchemas() {
        return "SHOW SCHEMAS";
    }

    /**
     * 构建显示表语句
     */
    public String buildShowTables() {
        return "SHOW TABLES";
    }

    /**
     * 构建显示列语句
     */
    public String buildShowColumns(String tableName) {
        return "SHOW COLUMNS FROM " + tableName;
    }

    /**
     * 构建显示函数语句
     */
    public String buildShowFunctions() {
        return "SHOW FUNCTIONS";
    }

    /**
     * 构建显示会话语句
     */
    public String buildShowSession() {
        return "SHOW SESSION";
    }
    /**
     * 构建设置会话参数语句
     */
    public String buildSetSession(String key, String value) {
        return "SET SESSION " + key + " = " + value;
    }

    /**
     * 构建重置会话参数语句
     */
    public String buildResetSession(String key) {
        return "RESET SESSION " + key;
    }

    /**
     * 构建设置配置属性语句
     */
    public String buildSetProperty(String key, String value) {
        return "SET " + key + " = " + value;
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
        return "EXPLAIN ANALYZE VERBOSE " + query;
    }

    /**
     * 构建类型转换语句
     */
    public String buildCast(String expression, String dataType) {
        return "CAST(" + expression + " AS " + dataType + ")";
    }

    /**
     * 构建近似计数不同语句
     */
    public String buildApproxDistinct(String column) {
        return "APPROX_DISTINCT(" + column + ")";
    }

    /**
     * 构建百分位数语句
     */
    public String buildApproxPercentile(String column, double percentile) {
        return "APPROX_PERCENTILE(" + column + ", " + percentile + ")";
    }

    /**
     * 构建直方图语句
     */
    public String buildHistogram(String column) {
        return "HISTOGRAM(" + column + ")";
    }


    /**
     * 构建数组字面量
     */
    public String buildArrayLiteral(List<String> values) {
        return "ARRAY[" + String.join(", ", values) + "]";
    }

    /**
     * 构建 Map 字面量
     */
    public String buildMapLiteral(Map<String, String> entries) {
        List<String> pairs = new ArrayList<>();
        for (Map.Entry<String, String> entry : entries.entrySet()) {
            pairs.add(entry.getKey() + ", " + entry.getValue());
        }
        return "MAP(ARRAY[" + String.join(", ", pairs) + "])";
    }

    /**
     * 构建正则表达式提取语句
     */
    public String buildRegexpExtract(String expression, String pattern, int group) {
        return "REGEXP_EXTRACT(" + expression + ", '" + pattern + "', " + group + ")";
    }

    /**
     * 构建正则表达式替换语句
     */
    public String buildRegexpReplace(String expression, String pattern, String replacement) {
        return "REGEXP_REPLACE(" + expression + ", '" + pattern + "', '" + replacement + "')";
    }

    /**
     * 构建 JSON 提取语句
     */
    public String buildJsonExtract(String jsonColumn, String jsonPath) {
        return "JSON_EXTRACT(" + jsonColumn + ", '" + jsonPath + "')";
    }

    /**
     * 构建 JSON 取值语句
     */
    public String buildJsonValue(String jsonColumn, String jsonPath) {
        return "JSON_VALUE(" + jsonColumn + ", '" + jsonPath + "')";
    }

    /**
     * 构建 JSON 查询语句
     */
    public String buildJsonQuery(String jsonColumn, String jsonPath) {
        return "JSON_QUERY(" + jsonColumn + ", '" + jsonPath + "')";
    }


    /**
     * 构建几何距离函数
     */
    public String buildGeometryDistance(String geom1, String geom2) {
        return "ST_Distance(" + geom1 + ", " + geom2 + ")";
    }

    /**
     * 构建几何相交函数
     */
    public String buildGeometryIntersects(String geom1, String geom2) {
        return "ST_Intersects(" + geom1 + ", " + geom2 + ")";
    }
}
