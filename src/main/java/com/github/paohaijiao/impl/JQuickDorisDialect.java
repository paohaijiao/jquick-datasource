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
import com.github.paohaijiao.dataType.JQuickDataTypeConverter;
import com.github.paohaijiao.dataType.impl.JQuickDorisDataTypeConverter;
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
 * Apache Doris 方言实现
 * 支持 Doris 实时分析型数据库的 SQL 语法特性
 * <p>
 * Doris 版本兼容性：
 * - Doris 1.2.x
 * - Doris 2.0.x
 * - Doris 2.1.x
 * <p>
 * Doris 与传统数据库的差异：
 * - 列式存储，MPP 架构
 * - 支持 Unique、Duplicate、Aggregate 三种数据模型
 * - 支持分区和分桶
 * - 支持物化视图
 * - 支持倒排索引和 BloomFilter 索引
 * - 支持 Stream Load、Broker Load 等多种数据导入方式
 * - 查询性能高，适合实时分析
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/15
 */
public class JQuickDorisDialect extends JQuickAbsSQLDialect {

    protected static final String DORIS_QUOTE = "`";

    @Override
    protected JQuickDataTypeConverter createDataTypeConvert() {
        return new JQuickDorisDataTypeConverter();
    }

    @Override
    protected String getQuoteKeyWord() {
        return DORIS_QUOTE;
    }

    @Override
    public String getAutoIncrementKeyword(JQuickTableDefinition tableDefinition) {
        // Doris 不支持自增列
        return "";
    }

    @Override
    protected void appendTableOptions(StringBuilder sql, JQuickTableDefinition table) {
        // Doris 表选项 - 核心特性
        if (table.getExtensions() != null) {
            // 数据模型（必须指定）
            if (table.getExtensions().containsKey("model")) {
                String model = table.getExtensions().get("model").toString().toUpperCase();
                sql.append("\n" + model);
            } else {
                // 默认使用 Duplicate 模型
                sql.append("\nDUPLICATE KEY");
            }
            // 分区
            if (table.getExtensions().containsKey("partitionBy")) {
                String partitionBy = table.getExtensions().get("partitionBy").toString();
                sql.append("\nPARTITION BY ").append(partitionBy);
            }
            // 分桶
            if (table.getExtensions().containsKey("distributedBy")) {
                String distributedBy = table.getExtensions().get("distributedBy").toString();
                sql.append("\nDISTRIBUTED BY HASH(").append(distributedBy).append(")");
                // 分桶数
                if (table.getExtensions().containsKey("buckets")) {
                    Integer buckets = (Integer) table.getExtensions().get("buckets");
                    if (buckets != null && buckets > 0) {
                        sql.append(" BUCKETS ").append(buckets);
                    }
                }
            } else if (table.getExtensions().containsKey("distributedByRandom")) {
                Boolean random = (Boolean) table.getExtensions().get("distributedByRandom");
                if (random != null && random) {
                    sql.append("\nDISTRIBUTED BY RANDOM");
                    if (table.getExtensions().containsKey("buckets")) {
                        Integer buckets = (Integer) table.getExtensions().get("buckets");
                        if (buckets != null && buckets > 0) {
                            sql.append(" BUCKETS ").append(buckets);
                        }
                    }
                }
            }
            // 表属性
            if (table.getExtensions().containsKey("properties")) {
                @SuppressWarnings("unchecked")
                Map<String, String> properties = (Map<String, String>) table.getExtensions().get("properties");
                if (properties != null && !properties.isEmpty()) {
                    sql.append("\nPROPERTIES (");
                    boolean first = true;
                    for (Map.Entry<String, String> entry : properties.entrySet()) {
                        if (!first) {
                            sql.append(", ");
                        }
                        sql.append("'").append(escapeString(entry.getKey())).append("' = '")
                                .append(escapeString(entry.getValue())).append("'");
                        first = false;
                    }
                    sql.append(")");
                }
            }
            // 表注释
            if (table.getComment() != null && !table.getComment().isEmpty()) {
                sql.append("\nCOMMENT '").append(escapeString(table.getComment())).append("'");
            }
            // 复制分配
            if (table.getExtensions().containsKey("replicationNum")) {
                Integer replicationNum = (Integer) table.getExtensions().get("replicationNum");
                if (replicationNum != null && replicationNum > 0) {
                    sql.append("\nPROPERTIES ('replication_num' = '").append(replicationNum).append("')");
                }
            }
            // 压缩
            if (table.getExtensions().containsKey("compression")) {
                String compression = table.getExtensions().get("compression").toString();
                sql.append("\nPROPERTIES ('compression' = '").append(compression.toUpperCase()).append("')");
            }
            // 存储介质
            if (table.getExtensions().containsKey("storageMedium")) {
                String storageMedium = table.getExtensions().get("storageMedium").toString();
                sql.append("\nPROPERTIES ('storage_medium' = '").append(storageMedium.toUpperCase()).append("')");
            }
            // 存储策略
            if (table.getExtensions().containsKey("storagePolicy")) {
                String storagePolicy = table.getExtensions().get("storagePolicy").toString();
                sql.append("\nPROPERTIES ('storage_policy' = '").append(storagePolicy).append("')");
            }
            // 动态分区
            if (table.getExtensions().containsKey("dynamicPartition")) {
                @SuppressWarnings("unchecked")
                Map<String, String> dynamicPartition = (Map<String, String>) table.getExtensions().get("dynamicPartition");
                if (dynamicPartition != null && !dynamicPartition.isEmpty()) {
                    sql.append("\nPROPERTIES (");
                    boolean first = true;
                    for (Map.Entry<String, String> entry : dynamicPartition.entrySet()) {
                        if (!first) {
                            sql.append(", ");
                        }
                        sql.append("'dynamic_partition.").append(entry.getKey()).append("' = '")
                                .append(escapeString(entry.getValue())).append("'");
                        first = false;
                    }
                    sql.append(")");
                }
            }
        }
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

            // 列注释
            if (column.getComment() != null && !column.getComment().isEmpty()) {
                sql.append(" COMMENT '").append(escapeString(column.getComment())).append("'");
            }
            // 聚合类型（Aggregate 模型）
            if (column.getExtensions() != null && column.getExtensions().containsKey("aggregationType")) {
                String aggType = column.getExtensions().get("aggregationType").toString();
                sql.append(" ").append(aggType.toUpperCase());
            }
            if (i < columns.size() - 1) {
                sql.append(",");
            }
            sql.append(NEW_LINE);
        }

        // 索引定义（BloomFilter、倒排索引等）
        if (table.getExtensions() != null && table.getExtensions().containsKey("indexes")) {
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> indexes = (List<Map<String, Object>>) table.getExtensions().get("indexes");
            if (indexes != null && !indexes.isEmpty()) {
                for (Map<String, Object> index : indexes) {
                    sql.append(COMMA).append(NEW_LINE);
                    sql.append(INDENT).append("INDEX ").append(quoteIdentifier(table, (String) index.get("name")));
                    sql.append(" (").append(formatColumnList(table, (List<String>) index.get("columns"))).append(")");
                    if (index.containsKey("type")) {
                        sql.append(" USING ").append(index.get("type").toString().toUpperCase());
                    }
                    if (index.containsKey("comment")) {
                        sql.append(" COMMENT '").append(escapeString(index.get("comment").toString())).append("'");
                    }
                }
            }
        }
        sql.append("\n)");
        appendTableOptions(sql, table);
        // 动态分区需要在 PROPERTIES 中设置
        if (table.getExtensions() != null && table.getExtensions().containsKey("dynamicPartition")) {
            // 已在 appendTableOptions 中处理
        }
        // 添加列注释的单独语句（Doris 不支持内联列注释）
        String columnComments = buildColumnComments(table);
        if (columnComments != null && !columnComments.isEmpty()) {
            sql.append(";\n").append(columnComments);
        }
        return sql.toString();
    }

    /**
     * 构建列注释语句
     */
    private String buildColumnComments(JQuickTableDefinition table) {
        StringBuilder sb = new StringBuilder();
        for (JQuickColumnDefinition column : table.getColumns()) {
            if (column.getComment() != null && !column.getComment().isEmpty()) {
                if (sb.length() > 0) {
                    sb.append(";\n");
                }
                sb.append("ALTER TABLE ").append(quoteIdentifier(table, table.getTableName()))
                        .append(" MODIFY COLUMN ").append(quoteIdentifier(table, column.getColumnName()))
                        .append(" COMMENT '").append(escapeString(column.getComment())).append("'");
            }
        }
        return sb.toString();
    }

    /**
     * 构建创建物化视图语句
     */
    public String buildCreateMaterializedView(String viewName, String tableName, String columns, String groupBy, boolean refreshAsync) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE MATERIALIZED VIEW ").append(viewName);
        sb.append(" AS SELECT ").append(columns);
        sb.append(" FROM ").append(tableName);
        if (groupBy != null && !groupBy.isEmpty()) {
            sb.append(" GROUP BY ").append(groupBy);
        }
        if (refreshAsync) {
            sb.append(" REFRESH ASYNC");
        }
        return sb.toString();
    }

    /**
     * 构建删除物化视图语句
     */
    public String buildDropMaterializedView(String viewName, boolean ifExists) {
        StringBuilder sb = new StringBuilder("DROP MATERIALIZED VIEW ");
        if (ifExists) {
            sb.append("IF EXISTS ");
        }
        sb.append(viewName);
        return sb.toString();
    }

    @Override
    public String buildColumnDefinition(JQuickTableDefinition table, JQuickColumnDefinition column) {
        StringBuilder def = new StringBuilder();
        def.append(quoteIdentifier(table, column.getColumnName())).append(SPACE);
        def.append(getDataTypeString(table, column.getDataType()));
        // 列注释
        if (column.getComment() != null && !column.getComment().isEmpty()) {
            def.append(" COMMENT '").append(escapeString(column.getComment())).append("'");
        }
        // 默认值
        if (column.getDefaultValue() != null) {
            def.append(" DEFAULT ").append(formatDefaultValue(column.getDefaultValue()));
        }

        return def.toString();
    }

    @Override
    protected void appendNullClause(StringBuilder def, JQuickColumnDefinition column) {
        // Doris 支持 NOT NULL
        if (!column.isNullable()) {
            def.append(" NOT NULL");
        }
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
        if ("CURRENT_TIMESTAMP".equals(upperValue) || "NOW()".equals(upperValue)) {
            return "CURRENT_TIMESTAMP";
        }
        if ("CURRENT_DATE".equals(upperValue)) {
            return "CURRENT_DATE";
        }
        return null;
    }

    @Override
    protected String formatBooleanDefault(boolean value) {
        return value ? "1" : "0";
    }

    @Override
    protected String convertForeignKeyAction(JQuickForeignKeyAction action) {
        // Doris 不支持外键约束
        return "";
    }

    @Override
    public String buildPrimaryKey(JQuickTableDefinition table, com.github.paohaijiao.extra.JQuickPrimaryKeyConstraint pk) {
        // Doris 在 Unique 模型中支持主键
        String sb = "PRIMARY KEY (" +
                formatColumnList(table, pk.getColumns()) +
                ")";
        return sb;
    }

    @Override
    public String buildUniqueConstraint(JQuickTableDefinition table, com.github.paohaijiao.extra.JQuickUniqueConstraint uc) {
        // Doris 不支持唯一约束
        return "";
    }

    @Override
    public String buildForeignKey(JQuickTableDefinition table, JQuickForeignKeyConstraint fk) {
        // Doris 不支持外键约束
        return "";
    }

    @Override
    public String buildModifyColumn(JQuickTableDefinition table, String tableName, JQuickColumnDefinition column) {
        // Doris 支持修改列
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ").append(quoteIdentifier(table, tableName));
        sb.append(" MODIFY COLUMN ").append(quoteIdentifier(table, column.getColumnName()));
        sb.append(" ").append(getDataTypeString(table, column.getDataType()));
        if (!column.isNullable()) {
            sb.append(" NOT NULL");
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
        // Doris 支持重命名列
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
        if (!newColumn.isNullable()) {
            sb.append(" NOT NULL");
        }
        if (newColumn.getDefaultValue() != null) {
            sb.append(" DEFAULT ").append(formatDefaultValue(newColumn.getDefaultValue()));
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
        if (column.getDefaultValue() != null) {
            sb.append(" DEFAULT ").append(formatDefaultValue(column.getDefaultValue()));
        }
        if (column.getComment() != null && !column.getComment().isEmpty()) {
            sb.append(" COMMENT '").append(escapeString(column.getComment())).append("'");
        }
        // 列位置
        if (column.getPosition() != null && column.getPosition().getAfterColumn() != null) {
            sb.append(" AFTER ").append(quoteIdentifier(table, column.getPosition().getAfterColumn()));
        }
        return sb.toString();
    }

    @Override
    public String buildDropColumn(JQuickTableDefinition table, String tableName, String columnName) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " DROP COLUMN " + quoteIdentifier(table, columnName);
    }

    @Override
    public String buildShowCreateTable(JQuickTableDefinition table, String tableName) {
        return "SHOW CREATE TABLE " + quoteIdentifier(table, tableName);
    }

    @Override
    public String buildDescribeTable(JQuickTableDefinition table, String tableName) {
        return "DESC " + quoteIdentifier(table, tableName);
    }

    @Override
    public String buildInsert(JQuickTableDefinition table, JQuickRow row) {
        // Doris 支持标准 INSERT
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

    @Override
    public String buildUpdate(JQuickTableDefinition table, JQuickRow row, String whereClause) {
        // Doris 支持 UPDATE（Unique 模型）
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
        // Doris 支持 DELETE
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
        // Doris 索引在 CREATE TABLE 中定义，或使用 ALTER TABLE ADD INDEX
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ").append("${tableName}");
        sb.append(" ADD INDEX ").append(quoteIdentifier(table, index.getIndexName()));
        sb.append(" (").append(formatColumnList(table, index.getColumns())).append(")");
        if (index.getType() != null) {
            sb.append(" USING ").append(index.getType().toUpperCase());
        }

        if (index.getComment() != null && !index.getComment().isEmpty()) {
            sb.append(" COMMENT '").append(escapeString(index.getComment())).append("'");
        }

        return sb.toString();
    }

    /**
     * 构建添加分区语句
     */
    public String buildAddPartition(JQuickTableDefinition table, String tableName, String partitionName, String partitionValue) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " ADD PARTITION " + partitionName + " VALUES [('" + partitionValue + "')]";
    }

    /**
     * 构建添加范围分区语句
     */
    public String buildAddRangePartition(JQuickTableDefinition table, String tableName, String partitionName, String startValue, String endValue) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " ADD PARTITION " + partitionName + " VALUES [('" + startValue + "'), ('" + endValue + "'))";
    }

    /**
     * 构建删除分区语句
     */
    public String buildDropPartition(JQuickTableDefinition table, String tableName, String partitionName) {return "ALTER TABLE " + quoteIdentifier(table, tableName) +
                " DROP PARTITION " + partitionName;
    }

    /**
     * 构建显示分区语句
     */
    public String buildShowPartitions(JQuickTableDefinition table, String tableName) {
        return "SHOW PARTITIONS FROM " + quoteIdentifier(table, tableName);
    }

    /**
     * 构建恢复分区语句
     */
    public String buildRecoverPartition(JQuickTableDefinition table, String tableName, String partitionName) {
        return "RECOVER PARTITION " + partitionName + " FROM " + quoteIdentifier(table, tableName);
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
        // 强制删除
        if (table.getExtensions() != null && table.getExtensions().containsKey("force")) {
            Boolean force = (Boolean) table.getExtensions().get("force");
            if (force != null && force) {
                sb.append(" FORCE");
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
        return "ALTER TABLE " + quoteIdentifier(table, oldName) + " RENAME " + quoteIdentifier(table, newName);
    }

    /**
     * 构建恢复表语句
     */
    public String buildRecoverTable(String tableName) {
        return "RECOVER TABLE " + tableName;
    }
    /**
     * 构建 Stream Load 导入语句（示例）
     */
    public String buildStreamLoad(String tableName, String filePath, Map<String, String> properties) {
        StringBuilder sb = new StringBuilder();
        sb.append("curl --location-trusted -u user:passwd -T ").append(filePath);
        sb.append(" -H \"column_separator:,\"");
        if (properties != null) {
            for (Map.Entry<String, String> entry : properties.entrySet()) {
                sb.append(" -H \"").append(entry.getKey()).append(":").append(entry.getValue()).append("\"");
            }
        }
        sb.append(" http://fe_host:http_port/api/{db}/").append(tableName).append("/_stream_load");
        return sb.toString();
    }

    /**
     * 构建 Broker Load 导入语句
     */
    public String buildBrokerLoad(String loadName, String tableName, String filePath, String brokerName) {
        String sb = "LOAD LABEL " + loadName + " (\n" +
                "  DATA INFILE(\"" + filePath + "\")\n" +
                "  INTO TABLE " + tableName + "\n" +
                "  COLUMNS TERMINATED BY \",\"\n" +
                ")\n" +
                "WITH BROKER '" + brokerName + "'\n" +
                "PROPERTIES (\n" +
                "  \"timeout\" = \"3600\"\n" +
                ")";

        return sb;
    }

    /**
     * 构建 INSERT INTO 导入语句
     */
    public String buildInsertIntoFromSelect(String tableName, String selectQuery) {
        return "INSERT INTO " + tableName + " " + selectQuery;
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
    public String buildDropDatabase(String databaseName, boolean ifExists) {
        StringBuilder sb = new StringBuilder("DROP DATABASE ");
        if (ifExists) {
            sb.append("IF EXISTS ");
        }
        sb.append(databaseName);
        // 强制删除
        if (ifExists) {
            sb.append(" FORCE");
        }

        return sb.toString();
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
     * 构建设置参数语句
     */
    public String buildSetParameter(String key, String value) {
        return "SET " + key + " = " + value;
    }

    /**
     * 构建启用/禁用查询重写
     */
    public String buildSetEnableRewrite(boolean enable) {
        return "SET enable_rewrite = " + (enable ? "1" : "0");
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
    public String buildExplainVerbose(String query) {
        return "EXPLAIN VERBOSE " + query;
    }

    /**
     * 构建图形化查询计划语句
     */
    public String buildExplainGraph(String query) {
        return "EXPLAIN GRAPH " + query;
    }

    /**
     * 构建分析查询语句
     */
    public String buildProfile() {
        return "SHOW QUERY PROFILE";
    }
    /**
     * 构建查询 FE 信息语句
     */
    public String buildShowFrontends() {
        return "SHOW FRONTENDS";
    }

    /**
     * 构建查询 BE 信息语句
     */
    public String buildShowBackends() {
        return "SHOW BACKENDS";
    }

    /**
     * 构建查询表数据量语句
     */
    public String buildGetTableDataSize(String tableName) {
        return "SHOW DATA FROM " + tableName;
    }
}
