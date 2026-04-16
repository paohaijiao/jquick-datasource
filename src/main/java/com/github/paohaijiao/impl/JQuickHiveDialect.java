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
import com.github.paohaijiao.dataType.impl.JQuickHiveDataTypeConverter;
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
 * Apache Hive 方言实现
 * 支持 Hive 数据仓库的 HiveQL 语法特性
 *
 * Hive 版本兼容性：
 * - Hive 1.2+
 * - Hive 2.x
 * - Hive 3.x
 *
 * 注意：Hive 与传统数据库的差异：
 * - 不支持行级 INSERT/UPDATE/DELETE（ACID 表除外）
 * - 不支持事务（ACID 表除外）
 * - 不支持自增列
 * - 不支持外键约束
 * - 不支持索引（有部分支持）
 * - 分区表是核心特性
 * - 支持存储格式（ORC, Parquet, Avro, TextFile 等）
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/15
 */
public class JQuickHiveDialect extends JQuickAbsSQLDialect {

    protected static final String HIVE_QUOTE = "`";

    @Override
    protected JQuickDataTypeConverter createDataTypeConvert() {
        return new JQuickHiveDataTypeConverter();
    }

    @Override
    protected String getQuoteKeyWord() {
        return HIVE_QUOTE;
    }

    @Override
    public String getAutoIncrementKeyword(JQuickTableDefinition tableDefinition) {
        // Hive 不支持自增列，返回空
        return "";
    }

    @Override
    protected void appendTableOptions(StringBuilder sql, JQuickTableDefinition table) {
        // Hive 表选项 - 这是 Hive 的核心特性
        if (table.getExtensions() != null) {
            // 存储格式
            if (table.getExtensions().containsKey("storedAs")) {
                String storedAs = table.getExtensions().get("storedAs").toString();
                sql.append("\nSTORED AS ").append(storedAs.toUpperCase());
            } else {
                // 默认存储格式
                sql.append("\nSTORED AS TEXTFILE");
            }
            // 行格式
            if (table.getExtensions().containsKey("rowFormat")) {
                String rowFormat = table.getExtensions().get("rowFormat").toString();
                sql.append("\nROW FORMAT ").append(rowFormat);
            } else if (table.getExtensions().containsKey("rowFormatDelimited")) {
                Map<String, String> delimited = getDelimitedFormat(table);
                if (!delimited.isEmpty()) {
                    sql.append("\nROW FORMAT DELIMITED");
                    for (Map.Entry<String, String> entry : delimited.entrySet()) {
                        sql.append("\n  ").append(entry.getKey()).append(" '").append(escapeString(entry.getValue())).append("'");
                    }
                }
            }

            // 位置（HDFS 路径）
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
        String database = connector.getSchema();      // 数据库名，默认 "default"
        String username = connector.getUsername();
        String password = connector.getPassword();
        if (host == null || host.trim().isEmpty()) {
            throw new IllegalStateException("Host is required for Hive connection");
        }
        String effectivePort = (port != null && !port.trim().isEmpty()) ? port : "10000";
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
        return url.toString();
    }

    /**
     * 获取分隔符格式配置
     */
    @SuppressWarnings("unchecked")
    private Map<String, String> getDelimitedFormat(JQuickTableDefinition table) {
        Map<String, String> result = new java.util.HashMap<>();
        if (table.getExtensions().containsKey("fieldsTerminatedBy")) {
            result.put("FIELDS TERMINATED BY", table.getExtensions().get("fieldsTerminatedBy").toString());
        }
        if (table.getExtensions().containsKey("linesTerminatedBy")) {
            result.put("LINES TERMINATED BY", table.getExtensions().get("linesTerminatedBy").toString());
        }
        if (table.getExtensions().containsKey("collectionItemsTerminatedBy")) {
            result.put("COLLECTION ITEMS TERMINATED BY", table.getExtensions().get("collectionItemsTerminatedBy").toString());
        }
        if (table.getExtensions().containsKey("mapKeysTerminatedBy")) {
            result.put("MAP KEYS TERMINATED BY", table.getExtensions().get("mapKeysTerminatedBy").toString());
        }
        if (table.getExtensions().containsKey("escapedBy")) {
            result.put("ESCAPED BY", table.getExtensions().get("escapedBy").toString());
        }
        return result;
    }

    @Override
    public String buildCreateTable(JQuickTableDefinition table) {
        StringBuilder sql = new StringBuilder();
        // 外部表
        boolean isExternal = false;
        if (table.getExtensions() != null && table.getExtensions().containsKey("external")) {
            Boolean external = (Boolean) table.getExtensions().get("external");
            if (external != null && external) {
                isExternal = true;
                sql.append("CREATE EXTERNAL TABLE ");
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
        sql.append(quoteIdentifier(table, table.getTableName()));
        // 分区列
        if (table.getExtensions() != null && table.getExtensions().containsKey("partitionedBy")) {
            @SuppressWarnings("unchecked")
            List<JQuickColumnDefinition> partitionColumns =
                    (List<JQuickColumnDefinition>) table.getExtensions().get("partitionedBy");
            if (partitionColumns != null && !partitionColumns.isEmpty()) {
                sql.append("\nPARTITIONED BY (");
                for (int i = 0; i < partitionColumns.size(); i++) {
                    JQuickColumnDefinition partitionCol = partitionColumns.get(i);
                    if (i > 0) {
                        sql.append(", ");
                    }
                    sql.append(quoteIdentifier(table, partitionCol.getColumnName()))
                            .append(" ").append(getDataTypeString(table, partitionCol.getDataType()));
                }
                sql.append(")");
            }
        }

        // 分桶列
        if (table.getExtensions() != null && table.getExtensions().containsKey("clusteredBy")) {
            @SuppressWarnings("unchecked")
            List<String> clusteredColumns = (List<String>) table.getExtensions().get("clusteredBy");
            if (clusteredColumns != null && !clusteredColumns.isEmpty()) {
                sql.append("\nCLUSTERED BY (");
                sql.append(clusteredColumns.stream().map(c -> quoteIdentifier(table, c)).collect(Collectors.joining(", ")));
                sql.append(")");
                // 分桶数
                if (table.getExtensions().containsKey("bucketCount")) {
                    Integer bucketCount = (Integer) table.getExtensions().get("bucketCount");
                    if (bucketCount != null && bucketCount > 0) {
                        sql.append("\nINTO ").append(bucketCount).append(" BUCKETS");
                    }
                }
                // 排序分桶
                if (table.getExtensions().containsKey("sortedBy")) {
                    @SuppressWarnings("unchecked")
                    List<String> sortedColumns = (List<String>) table.getExtensions().get("sortedBy");
                    if (sortedColumns != null && !sortedColumns.isEmpty()) {
                        sql.append("\nSORTED BY (");
                        sql.append(sortedColumns.stream().map(c -> quoteIdentifier(table, c)).collect(Collectors.joining(", ")));
                        sql.append(")");
                    }
                }
            }
        }
        // 普通列定义
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

    @Override
    public String buildColumnDefinition(JQuickTableDefinition table, JQuickColumnDefinition column) {
        // Hive 中列定义相对简单，不需要 NOT NULL 等约束
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
        // Hive 不支持 NOT NULL 约束（ACID 表除外），忽略
    }

    @Override
    protected void appendDefaultClause(StringBuilder def, JQuickColumnDefinition column) {
        // Hive 不支持 DEFAULT 值，忽略
    }

    @Override
    protected void appendColumnComment(StringBuilder def, JQuickColumnDefinition column) {
        // 已在 buildColumnDefinition 中处理
    }

    @Override
    protected String getSpecialDefaultKeyword(String value) {
        // Hive 不支持 DEFAULT
        return null;
    }

    @Override
    protected String formatBooleanDefault(boolean value) {
        return value ? "TRUE" : "FALSE";
    }

    @Override
    protected String convertForeignKeyAction(JQuickForeignKeyAction action) {
        // Hive 不支持外键约束
        return "";
    }

    @Override
    public String buildPrimaryKey(JQuickTableDefinition table,
                                  com.github.paohaijiao.extra.JQuickPrimaryKeyConstraint pk) {
        // Hive 不支持主键约束（ACID 表有有限支持）
        return "";
    }

    @Override
    public String buildUniqueConstraint(JQuickTableDefinition table,
                                        com.github.paohaijiao.extra.JQuickUniqueConstraint uc) {
        // Hive 不支持唯一约束
        return "";
    }

    @Override
    public String buildForeignKey(JQuickTableDefinition table, JQuickForeignKeyConstraint fk) {
        // Hive 不支持外键约束
        return "";
    }

    @Override
    public String buildModifyColumn(JQuickTableDefinition table, String tableName, JQuickColumnDefinition column) {
        // Hive 支持修改列
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ").append(quoteIdentifier(table, tableName));
        sb.append(" CHANGE COLUMN ").append(quoteIdentifier(table, column.getColumnName()));
        sb.append(" ").append(quoteIdentifier(table, column.getColumnName()));
        sb.append(" ").append(getDataTypeString(table, column.getDataType()));
        if (column.getComment() != null && !column.getComment().isEmpty()) {
            sb.append(" COMMENT '").append(escapeString(column.getComment())).append("'");
        }

        return sb.toString();
    }

    @Override
    public String buildChangeColumn(JQuickTableDefinition table, String tableName, String oldName, JQuickColumnDefinition newColumn) {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ").append(quoteIdentifier(table, tableName));
        sb.append(" CHANGE COLUMN ").append(quoteIdentifier(table, oldName));
        sb.append(" ").append(quoteIdentifier(table, newColumn.getColumnName()));
        sb.append(" ").append(getDataTypeString(table, newColumn.getDataType()));
        if (newColumn.getComment() != null && !newColumn.getComment().isEmpty()) {
            sb.append(" COMMENT '").append(escapeString(newColumn.getComment())).append("'");
        }
        return sb.toString();
    }

    @Override
    public String buildShowCreateTable(JQuickTableDefinition table, String tableName) {
        return "SHOW CREATE TABLE " + quoteIdentifier(table, tableName);
    }

    @Override
    public String buildDescribeTable(JQuickTableDefinition table, String tableName) {
        return "DESCRIBE FORMATTED " + quoteIdentifier(table, tableName);
    }

    @Override
    public String buildInsert(JQuickTableDefinition table, JQuickRow row) {
        // Hive 传统表不支持行级 INSERT，使用 INSERT INTO ... VALUES
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
     * 构建从查询插入的语句
     */
    public String buildInsertFromQuery(JQuickTableDefinition table, String query) {
        return "INSERT INTO " + quoteIdentifier(table, table.getTableName()) + "\n" + query;
    }

    /**
     * 构建覆盖写入语句
     */
    public String buildInsertOverwrite(JQuickTableDefinition table, String query) {
        return "INSERT OVERWRITE TABLE " + quoteIdentifier(table, table.getTableName()) + "\n" + query;
    }

    /**
     * 构建分区插入语句
     */
    public String buildInsertIntoPartition(JQuickTableDefinition table, Map<String, String> partition, JQuickRow row) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(quoteIdentifier(table, table.getTableName()));

        if (partition != null && !partition.isEmpty()) {
            sb.append(" PARTITION (");
            boolean first = true;
            for (Map.Entry<String, String> entry : partition.entrySet()) {
                if (!first) {
                    sb.append(", ");
                }
                sb.append(quoteIdentifier(table, entry.getKey())).append("='").append(escapeString(entry.getValue())).append("'");
                first = false;
            }
            sb.append(")");
        }

        sb.append(" VALUES (");
        List<String> values = new ArrayList<>();
        for (String col : row.keySet()) {
            values.add(formatValue(row.get(col)));
        }
        sb.append(String.join(", ", values));
        sb.append(")");

        return sb.toString();
    }

    @Override
    public String buildUpdate(JQuickTableDefinition table, JQuickRow row, String whereClause) {
        // Hive ACID 表支持 UPDATE
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
        // Hive ACID 表支持 DELETE
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
        // Hive 索引支持有限
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE INDEX ").append(quoteIdentifier(table, index.getIndexName()));
        sb.append(" ON TABLE ").append("${tableName}").append(" (");
        sb.append(formatColumnList(table, index.getColumns()));
        sb.append(")");
        if (index.hasExtension("indexType")) {
            String indexType = index.getExtension("indexType");
            sb.append(" AS '").append(indexType).append("'");
        }
        if (index.hasExtension("withDeferredRebuild")) {
            Boolean deferred = index.getExtension("withDeferredRebuild");
            if (deferred != null && deferred) {
                sb.append(" WITH DEFERRED REBUILD");
            }
        }
        if (index.hasExtension("indexTableName")) {
            String indexTableName = index.getExtension("indexTableName");
            sb.append(" IN TABLE ").append(quoteIdentifier(table, indexTableName));
        }

        return sb.toString();
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
                sb.append(quoteIdentifier(table, entry.getKey())).append("='")
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
                sb.append(quoteIdentifier(table, entry.getKey())).append("='")
                        .append(escapeString(entry.getValue())).append("'");
                first = false;
            }
            sb.append(")");
        }
        return sb.toString();
    }

    /**
     * 构建修复分区语句（MSCK）
     */
    public String buildRepairPartitions(JQuickTableDefinition table, String tableName) {
        return "MSCK REPAIR TABLE " + quoteIdentifier(table, tableName);
    }

    /**
     * 构建显示分区语句
     */
    public String buildShowPartitions(JQuickTableDefinition table, String tableName) {
        return "SHOW PARTITIONS " + quoteIdentifier(table, tableName);
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
     * 构建清空表语句（保留分区）
     */
    @Override
    public String buildTruncateTable(JQuickTableDefinition table, String tableName) {
        return "TRUNCATE TABLE " + quoteIdentifier(table, tableName);
    }

    /**
     * 构建清空分区语句
     */
    public String buildTruncatePartition(JQuickTableDefinition table, String tableName, Map<String, String> partition) {
        StringBuilder sb = new StringBuilder();
        sb.append("TRUNCATE TABLE ").append(quoteIdentifier(table, tableName));
        if (partition != null && !partition.isEmpty()) {
            sb.append(" PARTITION (");
            boolean first = true;
            for (Map.Entry<String, String> entry : partition.entrySet()) {
                if (!first) {
                    sb.append(", ");
                }
                sb.append(quoteIdentifier(table, entry.getKey())).append("='")
                        .append(escapeString(entry.getValue())).append("'");
                first = false;
            }
            sb.append(")");
        }

        return sb.toString();
    }

    /**
     * 构建重命名表语句
     */
    @Override
    public String buildRenameTable(JQuickTableDefinition table, String oldName, String newName) {
        return "ALTER TABLE " + quoteIdentifier(table, oldName) +
                " RENAME TO " + quoteIdentifier(table, newName);
    }
    /**
     * 构建分页查询语句（使用 LIMIT）
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
     * 构建分析表统计信息语句
     */
    public String buildAnalyzeTable(JQuickTableDefinition table, String tableName) {
        return "ANALYZE TABLE " + quoteIdentifier(table, tableName) + " COMPUTE STATISTICS";
    }

    /**
     * 构建分析分区统计信息语句
     */
    public String buildAnalyzePartition(JQuickTableDefinition table, String tableName, Map<String, String> partition) {
        StringBuilder sb = new StringBuilder();
        sb.append("ANALYZE TABLE ").append(quoteIdentifier(table, tableName));
        sb.append(" PARTITION (");
        boolean first = true;
        for (Map.Entry<String, String> entry : partition.entrySet()) {
            if (!first) {
                sb.append(", ");
            }
            sb.append(quoteIdentifier(table, entry.getKey())).append("='")
                    .append(escapeString(entry.getValue())).append("'");
            first = false;
        }
        sb.append(") COMPUTE STATISTICS");
        return sb.toString();
    }

    /**
     * 构建创建视图语句
     */
    public String buildCreateView(String viewName, String query) {
        return "CREATE VIEW " + viewName + " AS\n" + query;
    }

    /**
     * 构建创建物化视图语句（Hive 3.0+）
     */
    public String buildCreateMaterializedView(String viewName, String query) {
        return "CREATE MATERIALIZED VIEW " + viewName + " AS\n" + query;
    }


    /**
     * 构建导出数据语句
     */
    public String buildExportData(JQuickTableDefinition table, String tableName, String exportPath) {
        return "EXPORT TABLE " + quoteIdentifier(table, tableName) + " TO '" + escapeString(exportPath) + "'";
    }

    /**
     * 构建导入数据语句
     */
    public String buildImportData(JQuickTableDefinition table, String tableName, String importPath) {
        return "IMPORT TABLE " + quoteIdentifier(table, tableName) + " FROM '" + escapeString(importPath) + "'";
    }

    /**
     * 构建加载数据语句
     */
    public String buildLoadData(JQuickTableDefinition table, String tableName, String dataPath, boolean overwrite, Map<String, String> partition) {
        StringBuilder sb = new StringBuilder();
        sb.append("LOAD DATA ");
        if (overwrite) {
            sb.append("OVERWRITE ");
        }
        sb.append("INTO TABLE ").append(quoteIdentifier(table, tableName));
        if (partition != null && !partition.isEmpty()) {
            sb.append(" PARTITION (");
            boolean first = true;
            for (Map.Entry<String, String> entry : partition.entrySet()) {
                if (!first) {
                    sb.append(", ");
                }
                sb.append(quoteIdentifier(table, entry.getKey())).append("='")
                        .append(escapeString(entry.getValue())).append("'");
                first = false;
            }
            sb.append(")");
        }

        sb.append("\nFROM '").append(escapeString(dataPath)).append("'");

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
     * 构建显示函数语句
     */
    public String buildShowFunctions() {
        return "SHOW FUNCTIONS";
    }

    /**
     * 构建设置 Hive 参数语句
     */
    public String buildSetParameter(String key, String value) {
        return "SET " + key + "=" + value;
    }

    /**
     * 构建启用动态分区语句
     */
    public String buildEnableDynamicPartition(boolean enable) {
        if (enable) {
            return "SET hive.exec.dynamic.partition=true;\n" +
                    "SET hive.exec.dynamic.partition.mode=nonstrict";
        }
        return "SET hive.exec.dynamic.partition=false";
    }

    /**
     * 构建开启事务语句（ACID 表）
     */
    public String buildStartTransaction() {
        return "START TRANSACTION";
    }

    /**
     * 构建提交事务语句
     */
    public String buildCommitTransaction() {
        return "COMMIT";
    }

    /**
     * 构建回滚事务语句
     */
    public String buildRollbackTransaction() {
        return "ROLLBACK";
    }
}
