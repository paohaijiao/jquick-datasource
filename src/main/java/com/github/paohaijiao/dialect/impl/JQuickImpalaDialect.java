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
import com.github.paohaijiao.dataType.impl.JQuickImpalaDataTypeConverter;
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
 * Apache Impala 方言实现
 * 支持 Impala 查询引擎的 SQL 语法特性
 *
 * Impala 版本兼容性：
 * - Impala 2.x
 * - Impala 3.x
 * - Impala 4.x
 *
 * Impala 与传统数据库的差异：
 * - 不支持行级 INSERT/UPDATE/DELETE（支持批量 INSERT）
 * - 不支持事务
 * - 不支持自增列
 * - 不支持外键约束
 * - 不支持索引
 * - 分区表是核心特性
 * - 支持 Kudu 存储引擎的事务操作
 * - 查询性能极高，适合交互式分析
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/15
 */
public class JQuickImpalaDialect extends JQuickAbsSQLDialect {

    protected static final String IMPALA_QUOTE = "`";

    @Override
    protected JQuickDataTypeConverter createDataTypeConvert() {
        return new JQuickImpalaDataTypeConverter();
    }

    @Override
    protected String getQuoteKeyWord() {
        return IMPALA_QUOTE;
    }

    @Override
    public String getAutoIncrementKeyword(JQuickTableDefinition tableDefinition) {
        // Impala 不支持自增列
        return "";
    }

    @Override
    protected void appendTableOptions(StringBuilder sql, JQuickTableDefinition table) {
        // Impala 表选项
        if (table.getExtensions() != null) {
            // 存储格式
            if (table.getExtensions().containsKey("storedAs")) {
                String storedAs = table.getExtensions().get("storedAs").toString();
                sql.append("\nSTORED AS ").append(storedAs.toUpperCase());
            } else {
                // 默认存储格式为 PARQUET
                sql.append("\nSTORED AS PARQUET");
            }
            // 行格式（仅 TEXTFILE 格式需要）
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
                        sql.append("'").append(escapeString(entry.getKey())).append("='")
                                .append(escapeString(entry.getValue())).append("'");
                        first = false;
                    }
                    sql.append(")");
                }
            }

            // Kudu 表选项
            if (table.getExtensions().containsKey("kuduTableName")) {
                String kuduTableName = table.getExtensions().get("kuduTableName").toString();
                sql.append("\nTBLPROPERTIES ('kudu.table_name'='").append(escapeString(kuduTableName)).append("')");
            }

            // 压缩选项
            if (table.getExtensions().containsKey("compression")) {
                String compression = table.getExtensions().get("compression").toString();
                sql.append("\nTBLPROPERTIES ('parquet.compression'='").append(compression.toLowerCase()).append("')");
            }
        }
    }

    @Override
    public String getDriverClass(JQuickDataSourceConnector connector) {
        if (connector != null && connector.getDriverClass() != null && !connector.getDriverClass().trim().isEmpty()) {
            return connector.getDriverClass();
        }
        return "com.cloudera.impala.jdbc41.Driver";
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
            throw new IllegalStateException("Host is required for Impala connection");
        }
        String effectivePort = (port != null && !port.trim().isEmpty()) ? port : "21050";
        String effectiveDatabase = (database != null && !database.trim().isEmpty()) ? database : "default";
        StringBuilder url = new StringBuilder();
        url.append("jdbc:impala://").append(host).append(":").append(effectivePort);
        url.append("/").append(effectiveDatabase);
        boolean hasParams = false;
        if (username != null && !username.trim().isEmpty()) {
            url.append(";auth=noSasl");
            hasParams = true;
        }
        boolean useSsl = "true".equalsIgnoreCase(connector.getByKeyStr("ssl"));
        if (useSsl) {
            url.append(hasParams ? ";" : ";");
            url.append("ssl=true");
            hasParams = true;
        }
        boolean useKerberos = "true".equalsIgnoreCase(connector.getByKeyStr("kerberos"));
        if (useKerberos) {
            url.append(hasParams ? ";" : ";");
            url.append("auth=kerberos");
            String principal = connector.getByKeyStr("principal");
            if (principal != null && !principal.isEmpty()) {
                url.append(";principal=").append(principal);
            }
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
        // 分区列（Impala 分区语法与 Hive 略有不同）
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

            // Kudu 主键
            if (column.isPrimaryKey() && table.getExtensions() != null &&
                    "kudu".equalsIgnoreCase(getStorageFormat(table))) {
                sql.append(" PRIMARY KEY");
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
     * 获取存储格式
     */
    private String getStorageFormat(JQuickTableDefinition table) {
        if (table.getExtensions() != null && table.getExtensions().containsKey("storedAs")) {
            return table.getExtensions().get("storedAs").toString().toLowerCase();
        }
        return "parquet";
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
        // Impala 不支持 NOT NULL 约束
    }

    @Override
    protected void appendDefaultClause(StringBuilder def, JQuickColumnDefinition column) {
        // Impala 不支持 DEFAULT 值
    }

    @Override
    protected void appendColumnComment(StringBuilder def, JQuickColumnDefinition column) {
        // 已在 buildColumnDefinition 中处理
    }

    @Override
    protected String getSpecialDefaultKeyword(String value) {
        // Impala 不支持 DEFAULT
        return null;
    }

    @Override
    protected String formatBooleanDefault(boolean value) {
        return value ? "TRUE" : "FALSE";
    }

    @Override
    protected String convertForeignKeyAction(JQuickForeignKeyAction action) {
        // Impala 不支持外键约束
        return "";
    }

    @Override
    public String buildPrimaryKey(JQuickTableDefinition table, com.github.paohaijiao.extra.JQuickPrimaryKeyConstraint pk) {
        // Impala 不支持主键约束（Kudu 表除外）
        if (table.getExtensions() != null && "kudu".equalsIgnoreCase(getStorageFormat(table))) {
            StringBuilder sb = new StringBuilder();
            sb.append("PRIMARY KEY (");
            sb.append(formatColumnList(table, pk.getColumns()));
            sb.append(")");
            return sb.toString();
        }
        return "";
    }

    @Override
    public String buildUniqueConstraint(JQuickTableDefinition table, com.github.paohaijiao.extra.JQuickUniqueConstraint uc) {
        // Impala 不支持唯一约束
        return "";
    }

    @Override
    public String buildForeignKey(JQuickTableDefinition table, JQuickForeignKeyConstraint fk) {
        // Impala 不支持外键约束
        return "";
    }

    @Override
    public String buildModifyColumn(JQuickTableDefinition table, String tableName, JQuickColumnDefinition column) {
        // Impala 支持修改列
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
        // Impala 支持批量 INSERT，但单行插入也支持
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
        // Impala 支持 UPDATE（Kudu 表）
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
        // Impala 支持 DELETE（Kudu 表）
        StringBuilder sb = new StringBuilder();
        sb.append("DELETE FROM ").append(quoteIdentifier(table, table.getTableName()));
        if (whereClause != null && !whereClause.trim().isEmpty()) {
            sb.append(" WHERE ").append(whereClause);
        }
        return sb.toString();
    }

    /**
     * 构建 Upsert 语句（Kudu 表）
     */
    public String buildUpsert(JQuickTableDefinition table, JQuickRow row) {
        if (row == null || row.isEmpty() || table == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("UPSERT INTO ").append(quoteIdentifier(table, table.getTableName())).append(" (");
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
        // STRAIGHT_JOIN 提示
        if (table.getExtensions() != null && table.getExtensions().containsKey("straightJoin")) {
            Boolean straightJoin = (Boolean) table.getExtensions().get("straightJoin");
            if (straightJoin != null && straightJoin) {
                sb.append("STRAIGHT_JOIN ");
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
        // Impala 不支持索引
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
     * 构建显示分区语句
     */
    public String buildShowPartitions(JQuickTableDefinition table, String tableName) {
        return "SHOW PARTITIONS " + quoteIdentifier(table, tableName);
    }

    /**
     * 构建刷新分区语句
     */
    public String buildRefreshPartitions(JQuickTableDefinition table, String tableName) {
        return "REFRESH " + quoteIdentifier(table, tableName);
    }

    /**
     * 构建刷新指定分区语句
     */
    public String buildRefreshPartition(JQuickTableDefinition table, String tableName, Map<String, String> partition) {
        StringBuilder sb = new StringBuilder();
        sb.append("REFRESH ").append(quoteIdentifier(table, tableName));
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
     * 构建修复分区语句
     */
    public String buildRecoverPartitions(JQuickTableDefinition table, String tableName) {
        return "RECOVER PARTITIONS " + quoteIdentifier(table, tableName);
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
        return "ALTER TABLE " + quoteIdentifier(table, oldName) +
                " RENAME TO " + quoteIdentifier(table, newName);
    }


    /**
     * 构建计算表统计信息语句
     */
    public String buildComputeStats(JQuickTableDefinition table, String tableName) {
        return "COMPUTE STATS " + quoteIdentifier(table, tableName);
    }

    /**
     * 构建计算增量统计信息语句
     */
    public String buildComputeIncrementalStats(JQuickTableDefinition table, String tableName) {
        return "COMPUTE INCREMENTAL STATS " + quoteIdentifier(table, tableName);
    }

    /**
     * 构建删除表统计信息语句
     */
    public String buildDropStats(JQuickTableDefinition table, String tableName) {
        return "DROP STATS " + quoteIdentifier(table, tableName);
    }

    /**
     * 构建显示表统计信息语句
     */
    public String buildShowTableStats(JQuickTableDefinition table, String tableName) {
        return "SHOW TABLE STATS " + quoteIdentifier(table, tableName);
    }

    /**
     * 构建显示分区统计信息语句
     */
    public String buildShowPartitionStats(JQuickTableDefinition table, String tableName) {
        return "SHOW PARTITION STATS " + quoteIdentifier(table, tableName);
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
     * 构建分析表语句
     */
    public String buildAnalyzeTable(JQuickTableDefinition table, String tableName) {
        return "COMPUTE STATS " + quoteIdentifier(table, tableName);
    }
    /**
     * 构建加载数据语句
     */
    public String buildLoadData(JQuickTableDefinition table, String tableName, String dataPath,
                                boolean overwrite, Map<String, String> partition) {
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
     * 构建显示文件格式语句
     */
    public String buildShowFileFormats() {
        return "SHOW FILE FORMATS";
    }
    /**
     * 构建设置 Impala 参数语句
     */
    public String buildSetParameter(String key, String value) {
        return "SET " + key + "=" + value;
    }

    /**
     * 构建同步元数据语句
     */
    public String buildInvalidateMetadata() {
        return "INVALIDATE METADATA";
    }

    /**
     * 构建同步指定表元数据语句
     */
    public String buildInvalidateMetadata(String tableName) {
        return "INVALIDATE METADATA " + tableName;
    }

    /**
     * 构建刷新元数据语句
     */
    public String buildRefreshMetadata(String tableName) {
        return "REFRESH " + tableName;
    }

    /**
     * 构建启用/禁用查询提示
     */
    public String buildSetDisableCodegen(boolean disable) {
        return "SET DISABLE_CODEGEN=" + (disable ? "true" : "false");
    }

    /**
     * 构建设置并行度语句
     */
    public String buildSetParallelism(int parallelism) {
        return "SET MT_DOP=" + parallelism;
    }

    /**
     * 构建创建 Kudu 表语句
     */
    public String buildCreateKuduTable(JQuickTableDefinition table, List<String> primaryKeys) {
        if (table.getExtensions() == null) {
            table.setExtensions(new java.util.HashMap<>());
        }
        table.getExtensions().put("storedAs", "KUDU");
        StringBuilder sql = new StringBuilder();
        sql.append(buildCreateTable(table));
        if (primaryKeys != null && !primaryKeys.isEmpty()) {
            sql.append("\nPRIMARY KEY (");
            sql.append(primaryKeys.stream().map(c -> quoteIdentifier(table, c)).collect(Collectors.joining(", ")));
            sql.append(")");
        }
        // Kudu 表分区
        if (table.getExtensions().containsKey("kuduPartition")) {
            @SuppressWarnings("unchecked")
            Map<String, Object> partitionConfig = (Map<String, Object>) table.getExtensions().get("kuduPartition");
            if (partitionConfig != null && partitionConfig.containsKey("partitionByHash")) {
                Map<String, Object> hashPartition = (Map<String, Object>) partitionConfig.get("partitionByHash");
                sql.append("\nPARTITION BY HASH (");
                sql.append(formatColumnList(table, (List<String>) hashPartition.get("columns")));
                sql.append(") PARTITIONS ").append(hashPartition.get("partitions"));
            }
        }

        return sql.toString();
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
     * 构建分析查询语句
     */
    public String buildProfile() {
        return "PROFILE";
    }

    /**
     * 构建摘要查询语句
     */
    public String buildSummary() {
        return "SUMMARY";
    }
}