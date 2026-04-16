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
import com.github.paohaijiao.dataType.impl.JQuickClickHouseDataTypeConverter;
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
 * ClickHouse 方言实现
 * 支持 ClickHouse 列式存储数据库的 SQL 语法特性
 * <p>
 * ClickHouse 版本兼容性：
 * - ClickHouse 20.x - 24.x
 * <p>
 * ClickHouse 与传统数据库的差异：
 * - 列式存储，适合 OLAP 场景
 * - 支持分区表、合并树家族引擎
 * - 支持物化视图
 * - 不支持事务
 * - 不支持外键约束
 * - 不支持索引（有跳数索引）
 * - 支持分布式表
 * - 查询性能极高，适合大数据分析
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/15
 */
public class JQuickClickHouseDialect extends JQuickAbsSQLDialect {

    protected static final String CLICKHOUSE_QUOTE = "`";

    @Override
    protected JQuickDataTypeConverter createDataTypeConvert() {
        return new JQuickClickHouseDataTypeConverter();
    }

    @Override
    protected String getQuoteKeyWord() {
        return CLICKHOUSE_QUOTE;
    }

    @Override
    public String getAutoIncrementKeyword(JQuickTableDefinition tableDefinition) {
        // ClickHouse 不支持自增列，使用 DEFAULT 或物化列
        return "";
    }

    @Override
    protected void appendTableOptions(StringBuilder sql, JQuickTableDefinition table) {
        // ClickHouse 表选项 - 这是 ClickHouse 的核心特性
        if (table.getExtensions() != null) {
            // 表引擎（必须指定）
            if (table.getExtensions().containsKey("engine")) {
                String engine = table.getExtensions().get("engine").toString();
                sql.append("\nENGINE = ").append(engine);
            } else {
                // 默认使用 MergeTree
                sql.append("\nENGINE = MergeTree()");
            }
            // 分区键
            if (table.getExtensions().containsKey("partitionBy")) {
                String partitionBy = table.getExtensions().get("partitionBy").toString();
                sql.append("\nPARTITION BY ").append(partitionBy);
            }
            // 排序键（ClickHouse 中 ORDER BY 是必须的）
            if (table.getExtensions().containsKey("orderBy")) {
                String orderBy = table.getExtensions().get("orderBy").toString();
                sql.append("\nORDER BY ").append(orderBy);
            } else if (table.getExtensions().containsKey("primaryKey")) {
                // 如果没有 ORDER BY 但有 PRIMARY KEY，使用 PRIMARY KEY
                String primaryKey = table.getExtensions().get("primaryKey").toString();
                sql.append("\nORDER BY ").append(primaryKey);
            }
            // 主键
            if (table.getExtensions().containsKey("primaryKey")) {
                String primaryKey = table.getExtensions().get("primaryKey").toString();
                sql.append("\nPRIMARY KEY ").append(primaryKey);
            }
            // 采样键
            if (table.getExtensions().containsKey("sampleBy")) {
                String sampleBy = table.getExtensions().get("sampleBy").toString();
                sql.append("\nSAMPLE BY ").append(sampleBy);
            }
            // TTL（生存时间）
            if (table.getExtensions().containsKey("ttl")) {
                String ttl = table.getExtensions().get("ttl").toString();
                sql.append("\nTTL ").append(ttl);
            }
            // 设置参数
            if (table.getExtensions().containsKey("settings")) {
                @SuppressWarnings("unchecked")
                Map<String, String> settings = (Map<String, String>) table.getExtensions().get("settings");
                if (settings != null && !settings.isEmpty()) {
                    sql.append("\nSETTINGS ");
                    boolean first = true;
                    for (Map.Entry<String, String> entry : settings.entrySet()) {
                        if (!first) {
                            sql.append(", ");
                        }
                        sql.append(entry.getKey()).append(" = ").append(entry.getValue());
                        first = false;
                    }
                }
            }

            // 表注释
            if (table.getComment() != null && !table.getComment().isEmpty()) {
                sql.append("\nCOMMENT '").append(escapeString(table.getComment())).append("'");
            }
        }
    }

    @Override
    public String getDriverClass(JQuickDataSourceConnector connector) {
        if (connector != null && connector.getDriverClass() != null && !connector.getDriverClass().trim().isEmpty()) {
            return connector.getDriverClass();
        }
        return "com.clickhouse.jdbc.ClickHouseDriver";
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
            throw new IllegalStateException("Host is required for ClickHouse connection");
        }
        String effectivePort = (port != null && !port.trim().isEmpty()) ? port : "8123";
        StringBuilder url = new StringBuilder();
        url.append("jdbc:clickhouse://").append(host).append(":").append(effectivePort);
        if (database != null && !database.trim().isEmpty()) {
            url.append("/").append(database);
        } else {
            url.append("/");
        }
        boolean hasParams = false;
        if (username != null && !username.trim().isEmpty()) {
            url.append("?user=").append(username);
            hasParams = true;
        }
        if (password != null && !password.trim().isEmpty()) {
            url.append(hasParams ? "&" : "?").append("password=").append(password);
            hasParams = true;
        }
        if (!hasParams) {
            url.append("?");
            hasParams = true;
        } else {
            url.append("&");
        }
        url.append("socket_timeout=60000");
        url.append("&connect_timeout=30000");
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
        // 表名
        sql.append(quoteIdentifier(table, table.getTableName()));
        // 列定义
        sql.append(" (\n");
        List<JQuickColumnDefinition> columns = table.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            JQuickColumnDefinition column = columns.get(i);
            sql.append(INDENT).append(quoteIdentifier(table, column.getColumnName()))
                    .append(" ").append(getDataTypeString(table, column.getDataType()));

            // 默认值
            if (column.getDefaultValue() != null) {
                sql.append(" DEFAULT ").append(formatDefaultValue(column.getDefaultValue()));
            }

            // 物化列
            if (column.isMaterialized() && column.getMaterializedExpression() != null) {
                sql.append(" MATERIALIZED ").append(column.getMaterializedExpression());
            }
            // 别名列
            if (column.isAlias() && column.getAliasExpression() != null) {
                sql.append(" ALIAS ").append(column.getAliasExpression());
            }
            // 列注释
            if (column.getComment() != null && !column.getComment().isEmpty()) {
                sql.append(" COMMENT '").append(escapeString(column.getComment())).append("'");
            }
            // 压缩编解码器
            if (column.getExtensions() != null && column.getExtensions().containsKey("codec")) {
                String codec = column.getExtensions().get("codec").toString();
                sql.append(" CODEC(").append(codec).append(")");
            }
            // TTL
            if (column.getExtensions() != null && column.getExtensions().containsKey("ttl")) {
                String ttl = column.getExtensions().get("ttl").toString();
                sql.append(" TTL ").append(ttl);
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
     * 构建创建分布式表语句
     */
    public String buildCreateDistributedTable(JQuickTableDefinition table, String localTableName, String clusterName, String shardingKey) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE ").append(quoteIdentifier(table, table.getTableName()));
        sql.append(" AS ").append(quoteIdentifier(table, localTableName));
        sql.append("\nENGINE = Distributed(");
        sql.append("'").append(clusterName).append("', ");
        sql.append("'").append(getDatabaseName(table)).append("', ");
        sql.append("'").append(localTableName).append("'");
        if (shardingKey != null && !shardingKey.isEmpty()) {
            sql.append(", ").append(shardingKey);
        }
        sql.append(")");
        return sql.toString();
    }

    /**
     * 构建创建物化视图语句
     */
    public String buildCreateMaterializedView(String viewName, String engine, String populateType, String query) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE MATERIALIZED VIEW ").append(viewName);
        if (engine != null && !engine.isEmpty()) {
            sql.append("\nENGINE = ").append(engine);
        }
        if (populateType != null && !populateType.isEmpty()) {
            sql.append(" ").append(populateType.toUpperCase());
        }
        sql.append(" AS\n").append(query);

        return sql.toString();
    }

    /**
     * 获取数据库名
     */
    private String getDatabaseName(JQuickTableDefinition table) {
        if (table.getExtensions() != null && table.getExtensions().containsKey("database")) {
            return table.getExtensions().get("database").toString();
        }
        return "default";
    }

    @Override
    public String buildColumnDefinition(JQuickTableDefinition table, JQuickColumnDefinition column) {
        StringBuilder def = new StringBuilder();
        def.append(quoteIdentifier(table, column.getColumnName())).append(SPACE);
        def.append(getDataTypeString(table, column.getDataType()));
        // 默认值
        if (column.getDefaultValue() != null) {
            def.append(" DEFAULT ").append(formatDefaultValue(column.getDefaultValue()));
        }
        // 列注释
        if (column.getComment() != null && !column.getComment().isEmpty()) {
            def.append(" COMMENT '").append(escapeString(column.getComment())).append("'");
        }
        return def.toString();
    }

    @Override
    protected void appendNullClause(StringBuilder def, JQuickColumnDefinition column) {
        // ClickHouse 不支持 NOT NULL 约束（Nullable 类型除外）
    }

    @Override
    protected void appendDefaultClause(StringBuilder def, JQuickColumnDefinition column) {
        // 已在 buildColumnDefinition 中处理
    }

    @Override
    protected void appendColumnComment(StringBuilder def, JQuickColumnDefinition column) {
        // 已在 buildColumnDefinition 中处理
    }

    @Override
    protected String getSpecialDefaultKeyword(String value) {
        if (value == null) {
            return null;
        }
        String upperValue = value.toUpperCase();
        if ("NOW()".equals(upperValue) || "CURRENT_TIMESTAMP".equals(upperValue)) {
            return "now()";
        }
        if ("TODAY()".equals(upperValue) || "CURRENT_DATE".equals(upperValue)) {
            return "today()";
        }
        if ("YESTERDAY()".equals(upperValue)) {
            return "yesterday()";
        }
        return null;
    }

    @Override
    protected String formatBooleanDefault(boolean value) {
        return value ? "1" : "0";
    }

    @Override
    protected String convertForeignKeyAction(JQuickForeignKeyAction action) {
        // ClickHouse 不支持外键约束
        return "";
    }

    @Override
    public String buildPrimaryKey(JQuickTableDefinition table, com.github.paohaijiao.extra.JQuickPrimaryKeyConstraint pk) {
        // ClickHouse 主键在表选项中定义，不在列定义中
        return "";
    }

    @Override
    public String buildUniqueConstraint(JQuickTableDefinition table, com.github.paohaijiao.extra.JQuickUniqueConstraint uc) {
        // ClickHouse 不支持唯一约束
        return "";
    }

    @Override
    public String buildForeignKey(JQuickTableDefinition table, JQuickForeignKeyConstraint fk) {
        // ClickHouse 不支持外键约束
        return "";
    }

    @Override
    public String buildModifyColumn(JQuickTableDefinition table, String tableName, JQuickColumnDefinition column) {
        // ClickHouse 支持修改列
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ").append(quoteIdentifier(table, tableName));
        sb.append(" MODIFY COLUMN ").append(quoteIdentifier(table, column.getColumnName()));
        sb.append(" ").append(getDataTypeString(table, column.getDataType()));
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
        // ClickHouse 重命名列
        if (!oldName.equals(newColumn.getColumnName())) {
            sb.append("ALTER TABLE ").append(quoteIdentifier(table, tableName));
            sb.append(" RENAME COLUMN ").append(quoteIdentifier(table, oldName));
            sb.append(" TO ").append(quoteIdentifier(table, newColumn.getColumnName()));
            sb.append(";\n");
        }
        // 修改列
        sb.append("ALTER TABLE ").append(quoteIdentifier(table, tableName));
        sb.append(" MODIFY COLUMN ").append(quoteIdentifier(table, newColumn.getColumnName()));
        sb.append(" ").append(getDataTypeString(table, newColumn.getDataType()));
        if (newColumn.getDefaultValue() != null) {
            sb.append(" DEFAULT ").append(formatDefaultValue(newColumn.getDefaultValue()));
        }

        return sb.toString();
    }

    @Override
    public String buildShowCreateTable(JQuickTableDefinition table, String tableName) {
        return "SHOW CREATE TABLE " + quoteIdentifier(table, tableName);
    }

    @Override
    public String buildDescribeTable(JQuickTableDefinition table, String tableName) {
        return "DESCRIBE TABLE " + quoteIdentifier(table, tableName);
    }

    @Override
    public String buildInsert(JQuickTableDefinition table, JQuickRow row) {
        // ClickHouse 支持批量插入
        if (row == null || row.isEmpty() || table == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(quoteIdentifier(table, table.getTableName()));
        // 列名列表
        List<String> columns = new ArrayList<>(row.keySet());
        if (!columns.isEmpty()) {
            sb.append(" (").append(columns.stream()
                            .map(e -> quoteIdentifier(table, e))
                            .collect(Collectors.joining(", ")))
                    .append(")");
        }
        sb.append(" VALUES (");
        List<String> values = new ArrayList<>();
        for (String col : columns) {
            values.add(formatValue(row.get(col)));
        }
        sb.append(String.join(", ", values));
        sb.append(")");
        return sb.toString();
    }

    /**
     * 构建批量插入语句（ClickHouse 推荐）
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
        // ClickHouse 支持 UPDATE（需要表引擎支持）
        if (row == null || row.isEmpty() || table == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ").append(quoteIdentifier(table, table.getTableName()));
        sb.append(" UPDATE ");
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
        // ClickHouse 支持 DELETE
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ").append(quoteIdentifier(table, table.getTableName()));
        sb.append(" DELETE");
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
        // SAMPLE 子句
        if (table.getExtensions() != null && table.getExtensions().containsKey("sample")) {
            Object sample = table.getExtensions().get("sample");
            if (sample != null) {
                sb.append(" SAMPLE ").append(sample);
            }
        }
        // PREWHERE 子句（ClickHouse 优化）
        if (table.getExtensions() != null && table.getExtensions().containsKey("prewhere")) {
            String prewhere = table.getExtensions().get("prewhere").toString();
            sb.append(" PREWHERE ").append(prewhere);
        }
        if (whereClause != null && !whereClause.trim().isEmpty()) {
            sb.append(" WHERE ").append(whereClause);
        }
        // GROUP BY
        if (table.getExtensions() != null && table.getExtensions().containsKey("groupBy")) {
            String groupBy = table.getExtensions().get("groupBy").toString();
            sb.append(" GROUP BY ").append(groupBy);
        }
        // WITH TOTALS
        if (table.getExtensions() != null && table.getExtensions().containsKey("withTotals")) {
            Boolean withTotals = (Boolean) table.getExtensions().get("withTotals");
            if (withTotals != null && withTotals) {
                sb.append(" WITH TOTALS");
            }
        }
        // HAVING
        if (table.getExtensions() != null && table.getExtensions().containsKey("having")) {
            String having = table.getExtensions().get("having").toString();
            sb.append(" HAVING ").append(having);
        }
        // ORDER BY
        if (table.getExtensions() != null && table.getExtensions().containsKey("orderBy")) {
            String orderBy = table.getExtensions().get("orderBy").toString();
            sb.append(" ORDER BY ").append(orderBy);
        }
        // LIMIT BY
        if (table.getExtensions() != null && table.getExtensions().containsKey("limitBy")) {
            String limitBy = table.getExtensions().get("limitBy").toString();
            sb.append(" LIMIT ").append(limitBy);
        }
        return sb.toString();
    }

    @Override
    public String buildIndex(JQuickTableDefinition table, JQuickIndexDefinition index) {
        // ClickHouse 跳数索引
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ").append("${tableName}");
        sb.append(" ADD INDEX ").append(quoteIdentifier(table, index.getIndexName()));
        sb.append(" (").append(formatColumnList(table, index.getColumns())).append(")");
        if (index.getType() != null) {
            sb.append(" TYPE ").append(index.getType().toUpperCase());
        }
        if (index.getComment() != null && !index.getComment().isEmpty()) {
            sb.append(" GRANULARITY ").append(index.getComment());
        }
        return sb.toString();
    }
    /**
     * 构建删除分区语句
     */
    public String buildDropPartition(JQuickTableDefinition table, String tableName, String partitionValue) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " DROP PARTITION '" + partitionValue + "'";
    }

    /**
     * 构建删除分区语句（按条件）
     */
    public String buildDropPartitionWhere(JQuickTableDefinition table, String tableName, String whereClause) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " DROP PARTITION WHERE " + whereClause;
    }

    /**
     * 构建分离分区语句
     */
    public String buildDetachPartition(JQuickTableDefinition table, String tableName, String partitionValue) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " DETACH PARTITION '" + partitionValue + "'";
    }

    /**
     * 构建附加分区语句
     */
    public String buildAttachPartition(JQuickTableDefinition table, String tableName, String partitionValue) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " ATTACH PARTITION '" + partitionValue + "'";
    }

    /**
     * 构建移动分区语句
     */
    public String buildMovePartitionToTable(JQuickTableDefinition table, String tableName,
                                            String partitionValue, String destTableName) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " MOVE PARTITION '" + partitionValue + "' TO TABLE " + quoteIdentifier(table, destTableName);
    }

    /**
     * 构建替换分区语句
     */
    public String buildReplacePartition(JQuickTableDefinition table, String tableName, String srcPartition, String destTableName) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " REPLACE PARTITION '" + srcPartition + "' FROM " + quoteIdentifier(table, destTableName);
    }

    /**
     * 构建清除分区语句
     */
    public String buildClearPartition(JQuickTableDefinition table, String tableName, String partitionValue) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " CLEAR COLUMN IN PARTITION '" + partitionValue + "'";
    }

    /**
     * 构建冻结分区语句
     */
    public String buildFreezePartition(JQuickTableDefinition table, String tableName, String partitionValue) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " FREEZE PARTITION '" + partitionValue + "'";
    }

    /**
     * 构建显示分区语句
     */
    public String buildShowPartitions(JQuickTableDefinition table, String tableName) {
        return "SELECT partition, name, active FROM system.parts " + "WHERE table = '" + tableName + "' AND database = '" + getDatabaseName(table) + "'";
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
        return "RENAME TABLE " + quoteIdentifier(table, oldName) + " TO " + quoteIdentifier(table, newName);
    }

    /**
     * 构建优化表语句
     */
    public String buildOptimizeTable(JQuickTableDefinition table, String tableName) {
        return "OPTIMIZE TABLE " + quoteIdentifier(table, tableName);
    }

    /**
     * 构建优化分区语句
     */
    public String buildOptimizePartition(JQuickTableDefinition table, String tableName, String partitionValue) {
        return "OPTIMIZE TABLE " + quoteIdentifier(table, tableName) + " PARTITION '" + partitionValue + "'";
    }

    /**
     * 构建合并表分区语句
     */
    public String buildMergePartitions(JQuickTableDefinition table, String tableName, String partitionFrom, String partitionTo) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) +
                " MERGE PARTITION '" + partitionFrom + "' TO '" + partitionTo + "'";
    }
    /**
     * 构建删除物化视图语句
     */
    public String buildDropMaterializedView(String viewName, boolean ifExists) {
        StringBuilder sb = new StringBuilder("DROP VIEW ");
        if (ifExists) {
            sb.append("IF EXISTS ");
        }
        sb.append(viewName);
        return sb.toString();
    }
    /**
     * 构建创建字典语句
     */
    public String buildCreateDictionary(String dictName, String source, String layout, String lifetime, Map<String, String> attributes) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE DICTIONARY ").append(dictName).append("\n");
        sb.append("(\n");
        boolean first = true;
        for (Map.Entry<String, String> attr : attributes.entrySet()) {
            if (!first) {
                sb.append(",\n");
            }
            sb.append("  ").append(attr.getKey()).append(" ").append(attr.getValue());
            first = false;
        }
        sb.append("\n)\n");
        sb.append("PRIMARY KEY ").append(attributes.keySet().iterator().next()).append("\n");
        sb.append("SOURCE(").append(source).append(")\n");
        sb.append("LAYOUT(").append(layout).append(")\n");
        sb.append("LIFETIME(").append(lifetime).append(")");

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
     * 构建显示引擎语句
     */
    public String buildShowEngines() {
        return "SHOW ENGINES";
    }

    /**
     * 构建查询表大小语句
     */
    public String buildGetTableSize(String tableName) {
        return "SELECT sum(bytes) FROM system.parts WHERE table = '" + tableName + "' AND active = 1";
    }

    /**
     * 构建设置参数语句
     */
    public String buildSetParameter(String key, String value) {
        return "SET " + key + " = " + value;
    }

    /**
     * 构建设置查询超时语句
     */
    public String buildSetQueryTimeout(int seconds) {
        return "SET max_execution_time = " + seconds;
    }

    /**
     * 构建启用/禁用并行复制
     */
    public String buildSetParallelReplicas(int count) {
        return "SET max_parallel_replicas = " + count;
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
    public String buildExplainAst(String query) {
        return "EXPLAIN AST " + query;
    }

    /**
     * 构建语法树查询计划语句
     */
    public String buildExplainSyntax(String query) {
        return "EXPLAIN SYNTAX " + query;
    }

    /**
     * 构建管道查询计划语句
     */
    public String buildExplainPipeline(String query) {
        return "EXPLAIN PIPELINE " + query;
    }

    /**
     * 构建估算语句
     */
    public String buildEstimate(String query) {
        return "EXPLAIN ESTIMATE " + query;
    }

    /**
     * 构建导出数据语句
     */
    public String buildExportData(String tableName, String format, String path) {
        return "SELECT * FROM " + tableName + " INTO OUTFILE '" + path + "'\n" + "FORMAT " + format.toUpperCase();
    }

    /**
     * 构建导出数据语句（压缩）
     */
    public String buildExportDataCompressed(String tableName, String format, String path, String compression) {
        return "SELECT * FROM " + tableName + " INTO OUTFILE '" + path + "'\n" + "COMPRESSION '" + compression.toLowerCase() + "'\n" + "FORMAT " + format.toUpperCase();
    }
}
