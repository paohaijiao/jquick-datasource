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
import com.github.paohaijiao.dataType.impl.JQuickDrillDataTypeConverter;
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
 * Apache Drill 方言实现
 * 支持 Drill 分布式 SQL 查询引擎的语法特性
 * <p>
 * Drill 版本兼容性：
 * - Drill 1.18+
 * - Drill 1.19+
 * - Drill 1.20+
 * - Drill 1.21+
 * <p>
 * Drill 与传统数据库的差异：
 * - Schema-free 查询
 * - 支持多种数据源（文件系统、Hive、HBase、MongoDB、Kafka 等）
 * - 支持嵌套数据（JSON、Parquet、Avro）
 * - 支持动态类型
 * - 不支持传统 DDL（表结构通过存储插件定义）
 * - 主要用于查询，对数据写入支持有限
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/15
 */
public class JQuickDrillDialect extends JQuickAbsSQLDialect {

    protected static final String DRILL_QUOTE = "`";

    @Override
    protected JQuickDataTypeConverter createDataTypeConvert() {
        return new JQuickDrillDataTypeConverter();
    }

    @Override
    protected String getQuoteKeyWord() {
        return DRILL_QUOTE;
    }

    @Override
    public String getAutoIncrementKeyword(JQuickTableDefinition tableDefinition) {
        // Drill 不支持自增列
        return "";
    }

    @Override
    protected void appendTableOptions(StringBuilder sql, JQuickTableDefinition table) {
        // Drill 表选项（主要用于 CTAS 语句）
        if (table.getExtensions() != null) {

            // 存储格式
            if (table.getExtensions().containsKey("format")) {
                String format = table.getExtensions().get("format").toString();
                sql.append(" STORE AS ").append(format.toUpperCase());
            }

            // 存储位置
            if (table.getExtensions().containsKey("location")) {
                String location = table.getExtensions().get("location").toString();
                sql.append(" AT '").append(escapeString(location)).append("'");
            }

            // 分区
            if (table.getExtensions().containsKey("partitionBy")) {
                @SuppressWarnings("unchecked")
                List<String> partitionColumns = (List<String>) table.getExtensions().get("partitionBy");
                if (partitionColumns != null && !partitionColumns.isEmpty()) {
                    sql.append(" PARTITION BY (");
                    sql.append(partitionColumns.stream()
                            .map(c -> quoteIdentifier(table, c))
                            .collect(Collectors.joining(", ")));
                    sql.append(")");
                }
            }

            // 排序
            if (table.getExtensions().containsKey("orderBy")) {
                @SuppressWarnings("unchecked")
                List<String> orderColumns = (List<String>) table.getExtensions().get("orderBy");
                if (orderColumns != null && !orderColumns.isEmpty()) {
                    sql.append(" ORDER BY (");
                    sql.append(orderColumns.stream()
                            .map(c -> quoteIdentifier(table, c))
                            .collect(Collectors.joining(", ")));
                    sql.append(")");
                }
            }

            // 分布
            if (table.getExtensions().containsKey("distributeBy")) {
                @SuppressWarnings("unchecked")
                List<String> distributeColumns = (List<String>) table.getExtensions().get("distributeBy");
                if (distributeColumns != null && !distributeColumns.isEmpty()) {
                    sql.append(" DISTRIBUTE BY (");
                    sql.append(distributeColumns.stream()
                            .map(c -> quoteIdentifier(table, c))
                            .collect(Collectors.joining(", ")));
                    sql.append(")");
                }
            }

            // 表注释
            if (table.getComment() != null && !table.getComment().isEmpty()) {
                sql.append(" COMMENT '").append(escapeString(table.getComment())).append("'");
            }
        }
    }

    @Override
    public String getDriverClass(JQuickDataSourceConnector connector) {
        if (connector != null && connector.getDriverClass() != null && !connector.getDriverClass().trim().isEmpty()) {
            return connector.getDriverClass();
        }
        return "org.apache.drill.jdbc.Driver";
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
        String schema = connector.getSchema();      // 存储插件名称
        String username = connector.getUsername();
        String password = connector.getPassword();
        if (host == null || host.trim().isEmpty()) {
            throw new IllegalStateException("Host (ZooKeeper or Drillbit) is required for Drill connection");
        }
        String effectivePort = (port != null && !port.trim().isEmpty()) ? port : "2181";
        StringBuilder url = new StringBuilder();
        url.append("jdbc:drill:");
        boolean isDirectMode = "direct".equalsIgnoreCase(("connectionMode"));
        if (isDirectMode) {
            url.append("drillbit=").append(host);
            if (!host.contains(":")) {
                String drillbitPort = (port != null && !port.isEmpty()) ? port : "31010";
                url.append(":").append(drillbitPort);
            }
        } else {
            url.append("zk=").append(host);
            if (!host.contains(":")) {
                url.append(":").append(effectivePort);
            }
            String clusterId = connector.getByKeyStr("clusterId");
            if (clusterId != null && !clusterId.isEmpty()) {
                url.append("/drill/").append(clusterId);
            }
        }
        if (schema != null && !schema.trim().isEmpty()) {
            url.append(";schema=").append(schema);
        }
        if (username != null && !username.trim().isEmpty()) {
            url.append(";user=").append(username);
        }
        if (password != null && !password.trim().isEmpty()) {
            url.append(";password=").append(password);
        }

        return url.toString();
    }

    @Override
    public String buildCreateTable(JQuickTableDefinition table) {
        StringBuilder sql = new StringBuilder();
        // Drill 的 CREATE TABLE 主要用于 CTAS (Create Table As Select)
        sql.append("CREATE TABLE ");

        // IF NOT EXISTS
        if (table.getExtensions() != null && table.getExtensions().containsKey("ifNotExists")) {
            Boolean ifNotExists = (Boolean) table.getExtensions().get("ifNotExists");
            if (ifNotExists != null && ifNotExists) {
                sql.append("IF NOT EXISTS ");
            }
        }
        sql.append(quoteIdentifier(table, table.getTableName()));
        // 列定义（Drill 通常不需要预定义列，但支持）
        if (table.getColumns() != null && !table.getColumns().isEmpty()) {
            sql.append(" (\n");
            List<JQuickColumnDefinition> columns = table.getColumns();
            for (int i = 0; i < columns.size(); i++) {
                JQuickColumnDefinition column = columns.get(i);
                sql.append(INDENT).append(quoteIdentifier(table, column.getColumnName()))
                        .append(" ").append(getDataTypeString(table, column.getDataType()));

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
     * 构建 CTAS (Create Table As Select) 语句
     */
    public String buildCreateTableAsSelect(JQuickTableDefinition table, String query) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE ").append(quoteIdentifier(table, table.getTableName()));
        appendTableOptions(sql, table);
        sql.append(" AS\n").append(query);
        return sql.toString();
    }

    /**
     * 构建创建视图语句
     */
    public String buildCreateView(String viewName, String query) {
        return "CREATE OR REPLACE VIEW " + viewName + " AS\n" + query;
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

    @Override
    public String buildColumnDefinition(JQuickTableDefinition table, JQuickColumnDefinition column) {
        String def = quoteIdentifier(table, column.getColumnName()) + SPACE + getDataTypeString(table, column.getDataType());
        return def;
    }

    @Override
    protected void appendNullClause(StringBuilder def, JQuickColumnDefinition column) {
        // Drill 动态类型，不支持 NOT NULL
    }

    @Override
    protected void appendDefaultClause(StringBuilder def, JQuickColumnDefinition column) {
        // Drill 不支持 DEFAULT
    }

    @Override
    protected void appendColumnComment(StringBuilder def, JQuickColumnDefinition column) {
        // Drill 不支持列注释
    }

    @Override
    protected String getSpecialDefaultKeyword(String value) {
        // Drill 不支持 DEFAULT
        return null;
    }

    @Override
    protected String formatBooleanDefault(boolean value) {
        return value ? "TRUE" : "FALSE";
    }

    @Override
    protected String convertForeignKeyAction(JQuickForeignKeyAction action) {
        // Drill 不支持外键约束
        return "";
    }

    @Override
    public String buildPrimaryKey(JQuickTableDefinition table, com.github.paohaijiao.extra.JQuickPrimaryKeyConstraint pk) {
        // Drill 不支持主键约束
        return "";
    }

    @Override
    public String buildUniqueConstraint(JQuickTableDefinition table, com.github.paohaijiao.extra.JQuickUniqueConstraint uc) {
        // Drill 不支持唯一约束
        return "";
    }

    @Override
    public String buildForeignKey(JQuickTableDefinition table, JQuickForeignKeyConstraint fk) {
        // Drill 不支持外键约束
        return "";
    }

    @Override
    public String buildModifyColumn(JQuickTableDefinition table, String tableName, JQuickColumnDefinition column) {
        // Drill 不支持修改列
        return "-- Drill does not support ALTER TABLE MODIFY COLUMN";
    }

    @Override
    public String buildChangeColumn(JQuickTableDefinition table, String tableName, String oldName, JQuickColumnDefinition newColumn) {
        // Drill 不支持修改
        return "-- Drill does not support ALTER TABLE CHANGE COLUMN";
    }

    @Override
    public String buildAddColumn(JQuickTableDefinition table, String tableName, JQuickColumnDefinition column) {
        // Drill 不支持添加列
        return "-- Drill does not support ALTER TABLE ADD COLUMN";
    }

    @Override
    public String buildDropColumn(JQuickTableDefinition table, String tableName, String columnName) {
        // Drill 不支持删除列
        return "-- Drill does not support ALTER TABLE DROP COLUMN";
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
        // Drill 支持 INSERT
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

    /**
     * 构建从查询插入的语句
     */
    public String buildInsertFromQuery(JQuickTableDefinition table, String query) {
        return "INSERT INTO " + quoteIdentifier(table, table.getTableName()) + "\n" + query;
    }

    @Override
    public String buildUpdate(JQuickTableDefinition table, JQuickRow row, String whereClause) {
        // Drill 支持 UPDATE
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
        // Drill 支持 DELETE
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
        // Drill 不支持索引
        return "";
    }

    /**
     * 构建查询文件系统数据
     */
    public String buildSelectFromFile(String filePath, String format) {
        return "SELECT * FROM dfs.`" + filePath + "`";
    }

    /**
     * 构建查询 JSON 文件
     */
    public String buildSelectFromJson(String filePath) {
        return "SELECT * FROM dfs.`" + filePath + "`";
    }

    /**
     * 构建查询 Parquet 文件
     */
    public String buildSelectFromParquet(String filePath) {
        return "SELECT * FROM dfs.`" + filePath + "`";
    }

    /**
     * 构建查询 CSV 文件
     */
    public String buildSelectFromCsv(String filePath, String delimiter, boolean hasHeader) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT * FROM dfs.`").append(filePath).append("`");

        if (delimiter != null || hasHeader) {
            sb.append(" (");
            boolean first = true;
            if (delimiter != null) {
                sb.append("type => 'csv', fieldDelimiter => '").append(delimiter).append("'");
                first = false;
            }
            if (hasHeader) {
                if (!first) {
                    sb.append(", ");
                }
                sb.append("extractHeader => true");
            }
            sb.append(")");
        }

        return sb.toString();
    }

    /**
     * 构建查询 Hive 表
     */
    public String buildSelectFromHive(String tableName) {
        return "SELECT * FROM hive.`" + tableName + "`";
    }

    /**
     * 构建查询 MongoDB 集合
     */
    public String buildSelectFromMongoDB(String collectionName) {
        return "SELECT * FROM mongo.`" + collectionName + "`";
    }

    /**
     * 构建查询 JDBC 数据源
     */
    public String buildSelectFromJdbc(String storageName, String tableName) {
        return "SELECT * FROM " + storageName + ".`" + tableName + "`";
    }

    /**
     * 构建展平数组查询
     */
    public String buildFlattenQuery(String tableName, String arrayColumn) {
        return "SELECT * FROM " + tableName + ", UNNEST(" + arrayColumn + ") AS t(" + arrayColumn + "_item)";
    }

    /**
     * 构建 JSON 字段提取查询
     */
    public String buildJsonExtractQuery(String tableName, String jsonColumn, String jsonPath) {
        return "SELECT " + jsonColumn + "." + jsonPath + " FROM " + tableName;
    }

    /**
     * 构建 KV 查询
     */
    public String buildKVQuery(String tableName, String keyColumn, String valueColumn, String key) {
        return "SELECT " + valueColumn + " FROM " + tableName + " WHERE " + keyColumn + " = '" + key + "'";
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
     * 构建显示存储插件语句
     */
    public String buildShowStoragePlugins() {
        return "SHOW PLUGINS";
    }

    /**
     * 构建显示函数语句
     */
    public String buildShowFunctions() {
        return "SHOW FUNCTIONS";
    }

    /**
     * 构建查询文件系统信息语句
     */
    public String buildShowFiles(String path) {
        return "SHOW FILES FROM dfs.`" + path + "`";
    }

    /**
     * 构建设置参数语句
     */
    public String buildSetParameter(String key, String value) {
        return "ALTER SESSION SET `" + key + "` = " + value;
    }

    /**
     * 构建重置参数语句
     */
    public String buildResetParameter(String key) {
        return "ALTER SESSION RESET `" + key + "`";
    }

    /**
     * 构建设置查询超时语句
     */
    public String buildSetQueryTimeout(int seconds) {
        return "ALTER SESSION SET `planner.width.max_per_node` = " + seconds;
    }

    /**
     * 构建查询计划语句
     */
    public String buildExplain(String query) {
        return "EXPLAIN PLAN FOR " + query;
    }

    /**
     * 构建详细查询计划语句
     */
    public String buildExplainDetailed(String query) {
        return "EXPLAIN PLAN WITH IMPLEMENTATION FOR " + query;
    }

    /**
     * 构建查询分析语句
     */
    public String buildProfile(String queryId) {
        return "SELECT * FROM sys.profiles WHERE query_id = '" + queryId + "'";
    }

    /**
     * 构建显示查询配置文件语句
     */
    public String buildShowQueries() {
        return "SELECT * FROM sys.profiles ORDER BY start_time DESC LIMIT 20";
    }


    /**
     * 构建创建存储插件语句
     */
    public String buildCreateStoragePlugin(String pluginName, String pluginConfig) {
        return "CREATE STORAGE PLUGIN `" + pluginName + "` TYPE '" + pluginConfig + "'";
    }

    /**
     * 构建删除存储插件语句
     */
    public String buildDropStoragePlugin(String pluginName) {
        return "DROP STORAGE PLUGIN `" + pluginName + "`";
    }

    /**
     * 构建使用存储插件语句
     */
    public String buildUseStoragePlugin(String pluginName) {
        return "USE " + pluginName;
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
        // 是否删除数据文件
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
     * 构建 CAST 表达式
     */
    public String buildCast(String expression, String dataType) {
        return "CAST(" + expression + " AS " + dataType + ")";
    }

    /**
     * 构建 CONVERT_TO 函数（转换为特定格式）
     */
    public String buildConvertTo(String expression, String format) {
        return "CONVERT_TO(" + expression + ", '" + format.toUpperCase() + "')";
    }

    /**
     * 构建 CONVERT_FROM 函数（从特定格式转换）
     */
    public String buildConvertFrom(String expression, String format) {
        return "CONVERT_FROM(" + expression + ", '" + format.toUpperCase() + "')";
    }


    /**
     * 构建字符串拆分函数
     */
    public String buildSplitString(String expression, String delimiter) {
        return "SPLIT(" + expression + ", '" + delimiter + "')";
    }

    /**
     * 构建字符串拼接函数
     */
    public String buildConcat(List<String> expressions) {
        return "CONCAT(" + String.join(", ", expressions) + ")";
    }



    /**
     * 构建近似计数不同函数
     */
    public String buildApproxCountDistinct(String column) {
        return "APPROX_COUNT_DISTINCT(" + column + ")";
    }

    /**
     * 构建统计函数
     */
    public String buildStatistics(String column, String statType) {
        return "STATISTICS(" + column + ", '" + statType + "')";
    }


    /**
     * 构建查询版本语句
     */
    public String buildVersion() {
        return "SELECT * FROM sys.version";
    }

    /**
     * 构建查询内存信息语句
     */
    public String buildMemoryInfo() {
        return "SELECT * FROM sys.memory";
    }

    /**
     * 构建查询选项语句
     */
    public String buildShowOptions() {
        return "SHOW ALL";
    }

    /**
     * 构建查询当前会话信息语句
     */
    public String buildShowSession() {
        return "SELECT * FROM sys.options WHERE type = 'SESSION'";
    }
}
