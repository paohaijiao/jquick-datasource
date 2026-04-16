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
import com.github.paohaijiao.dataType.enums.JQuickDataTypeFamily;
import com.github.paohaijiao.dataType.impl.JQuickBigQueryDataTypeConverter;
import com.github.paohaijiao.dialect.JQuickAbsSQLDialect;
import com.github.paohaijiao.enums.JQuickForeignKeyAction;
import com.github.paohaijiao.extra.JQuickForeignKeyConstraint;
import com.github.paohaijiao.extra.JQuickIndexDefinition;
import com.github.paohaijiao.extra.JQuickPrimaryKeyConstraint;
import com.github.paohaijiao.extra.JQuickUniqueConstraint;
import com.github.paohaijiao.row.JQuickRow;
import com.github.paohaijiao.table.JQuickTableDefinition;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Google BigQuery 方言实现
 * 支持 BigQuery 云数据仓库的 SQL 语法特性
 * <p>
 * BigQuery 版本兼容性：
 * - Google SQL 方言
 * <p>
 * BigQuery 特性：
 * - 列式存储，自动压缩
 * - 支持分区表和聚簇表
 * - 支持物化视图
 * - 支持脚本和存储过程
 * - 支持 JSON 和 ARRAY 类型
 * - 支持通配符表查询
 * - 按需或容量定价
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/15
 */
public class JQuickBigQueryDialect extends JQuickAbsSQLDialect {

    public static final String PARTITION_TYPE_DAY = "DAY";
    public static final String PARTITION_TYPE_HOUR = "HOUR";
    public static final String PARTITION_TYPE_MONTH = "MONTH";
    public static final String PARTITION_TYPE_YEAR = "YEAR";
    public static final String CLUSTER_TYPE_UNSPECIFIED = "CLUSTER_TYPE_UNSPECIFIED";
    public static final String CLUSTER_TYPE_KMEANS = "KMEANS";
    protected static final String BIGQUERY_QUOTE = "`";
    @Override
    protected JQuickDataTypeConverter createDataTypeConvert() {
        return new JQuickBigQueryDataTypeConverter();
    }

    @Override
    protected String getQuoteKeyWord() {
        return BIGQUERY_QUOTE;
    }

    @Override
    public String getAutoIncrementKeyword(JQuickTableDefinition tableDefinition) {
        return "";
    }

    @Override
    protected void appendTableOptions(StringBuilder sql, JQuickTableDefinition table) {
        if (table.getExtensions() == null) {
            return;
        }

        Map<String, Object> ext = table.getExtensions();
        if (ext.containsKey("tableType")) {
            String tableType = ext.get("tableType").toString();
            if ("EXTERNAL".equalsIgnoreCase(tableType)) {
                sql.append("\nOPTIONS (");
                // 外部表配置
                if (ext.containsKey("externalTableUri")) {
                    sql.append("\n  uri = '").append(ext.get("externalTableUri")).append("'");
                }
                if (ext.containsKey("externalTableFormat")) {
                    sql.append(",\n  format = '").append(ext.get("externalTableFormat")).append("'");
                }
                if (ext.containsKey("externalTableSkipLeadingRows")) {
                    sql.append(",\n  skip_leading_rows = ").append(ext.get("externalTableSkipLeadingRows"));
                }
                sql.append("\n)");
            } else if ("SNAPSHOT".equalsIgnoreCase(tableType)) {
                // 快照表
                if (ext.containsKey("sourceTable")) {
                    sql.append("\nCLONE ").append(ext.get("sourceTable"));
                }
            }
        }

        // 分区配置
        if (ext.containsKey("partitionBy")) {
            sql.append("\nPARTITION BY ").append(ext.get("partitionBy"));
            // 分区字段类型
            if (ext.containsKey("partitionType")) {
                String partitionType = ext.get("partitionType").toString();
                sql.append(" (").append(partitionType).append(")");
            }
            // 分区范围
            if (ext.containsKey("partitionRange")) {
                @SuppressWarnings("unchecked")
                Map<String, Integer> range = (Map<String, Integer>) ext.get("partitionRange");
                if (range != null && range.containsKey("start") &&
                        range.containsKey("end") && range.containsKey("interval")) {
                    sql.append("\nRANGE PARTITION (")
                            .append(range.get("start")).append(", ")
                            .append(range.get("end")).append(", ")
                            .append(range.get("interval")).append(")");
                }
            }
        }
        // 聚簇配置
        if (ext.containsKey("clusterBy")) {
            @SuppressWarnings("unchecked")
            List<String> clusterColumns = (List<String>) ext.get("clusterBy");
            if (clusterColumns != null && !clusterColumns.isEmpty()) {
                sql.append("\nCLUSTER BY ");
                sql.append(clusterColumns.stream()
                        .map(c -> quoteIdentifier(table, c))
                        .collect(Collectors.joining(", ")));
            }
        }
        // 表标签
        if (ext.containsKey("labels")) {
            @SuppressWarnings("unchecked")
            Map<String, String> labels = (Map<String, String>) ext.get("labels");
            if (labels != null && !labels.isEmpty()) {
                sql.append("\nOPTIONS (");
                sql.append("labels = [");
                boolean first = true;
                for (Map.Entry<String, String> entry : labels.entrySet()) {
                    if (!first) sql.append(", ");
                    sql.append("('").append(entry.getKey()).append("', '")
                            .append(escapeString(entry.getValue())).append("')");
                    first = false;
                }
                sql.append("]");
                sql.append("\n)");
            }
        }
        // 表描述
        if (table.getComment() != null && !table.getComment().isEmpty()) {
            if (ext.containsKey("options")) {
                sql.append(",\n  description = '").append(escapeString(table.getComment())).append("'");
            } else {
                sql.append("\nOPTIONS (\n  description = '")
                        .append(escapeString(table.getComment())).append("'\n)");
            }
        }
        // 过期时间
        if (ext.containsKey("expirationDays")) {
            Integer days = (Integer) ext.get("expirationDays");
            if (days != null && days > 0) {
                sql.append("\nOPTIONS (\n  expiration_timestamp = TIMESTAMP_ADD(CURRENT_TIMESTAMP(), INTERVAL ")
                        .append(days).append(" DAY)\n)");
            }
        }
        // 数据保留
        if (ext.containsKey("partitionExpirationDays")) {
            Integer days = (Integer) ext.get("partitionExpirationDays");
            if (days != null && days > 0) {
                sql.append("\nOPTIONS (\n  partition_expiration_days = ").append(days).append("\n)");
            }
        }
        // 表要求分区过滤
        if (ext.containsKey("requirePartitionFilter")) {
            Boolean require = (Boolean) ext.get("requirePartitionFilter");
            if (require != null && require) {
                sql.append("\nOPTIONS (\n  require_partition_filter = TRUE\n)");
            }
        }
        // 友好名称
        if (ext.containsKey("friendlyName")) {
            String friendlyName = ext.get("friendlyName").toString();
            sql.append("\nOPTIONS (\n  friendly_name = '").append(escapeString(friendlyName)).append("'\n)");
        }
    }

    @Override
    public String getDriverClass(JQuickDataSourceConnector connector) {
        if(null!=connector){
            return connector.getDriverClass();
        }
        return "com.simba.googlebigquery.jdbc42.Driver";
    }

    @Override
    public String getUrl(JQuickDataSourceConnector connector) {
        if (connector == null) {
            throw new IllegalArgumentException("Connector cannot be null");
        }
        if (connector.getUrl() != null && !connector.getUrl().trim().isEmpty()) {
            return connector.getUrl();
        }
        String projectId = connector.getSchema();
        String host = connector.getHost();
        String port = connector.getPort();
        String username = connector.getUsername();
        String password = connector.getPassword();
        if (projectId == null || projectId.trim().isEmpty()) {
            throw new IllegalStateException("Project ID (schema) is required for BigQuery connection");
        }
        StringBuilder url = new StringBuilder();
        if (host != null && !host.trim().isEmpty()) {
            url.append("jdbc:bigquery://").append(host);
            if (port != null && !port.trim().isEmpty()) {
                url.append(":").append(port);
            }
        } else {
            url.append("jdbc:bigquery://https://www.googleapis.com/bigquery/v2:443");
        }
        if (!url.toString().endsWith(";")) {
            url.append(";");
        }
        url.append("ProjectId=").append(projectId).append(";");

        if (username != null && !username.trim().isEmpty() && password != null && !password.trim().isEmpty()) {
            url.append("OAuthType=0;");
            url.append("OAuthServiceAcctEmail=").append(username).append(";");
            url.append("OAuthPvtKeyPath=").append(password).append(";");
        } else if (username != null && !username.trim().isEmpty()) {
            url.append("OAuthType=1;");
        } else {
            url.append("OAuthType=3;");
        }
        url.append("Timeout=3600;");
        url.append("AllowLargeResults=1;");
        return url.toString();
    }

    @Override
    public String buildCreateTable(JQuickTableDefinition table) {
        StringBuilder sql = new StringBuilder();
        // 临时表
        if (isTemporary(table)) {
            sql.append("CREATE TEMP TABLE ");
        } else {
            sql.append("CREATE TABLE ");
        }
        // IF NOT EXISTS
        if (ifNotExists(table)) {
            sql.append("IF NOT EXISTS ");
        }
        // 数据集限定名
        if (table.getExtensions() != null && table.getExtensions().containsKey("dataset")) {
            sql.append(quoteIdentifier(table, table.getExtensions().get("dataset").toString()))
                    .append(".");
        }
        sql.append(quoteIdentifier(table, table.getTableName()));
        // 定义
        sql.append(" (\n");
        List<JQuickColumnDefinition> columns = table.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            JQuickColumnDefinition column = columns.get(i);
            sql.append(INDENT).append(quoteIdentifier(table, column.getColumnName()))
                    .append(" ").append(getDataTypeString(table, column.getDataType()));
            // 模式选项
            if (!column.isNullable()) {
                sql.append(" NOT NULL");
            }
            // 列描述
            if (column.getComment() != null && !column.getComment().isEmpty()) {
                sql.append(" OPTIONS(description = '")
                        .append(escapeString(column.getComment())).append("')");
            }
            if (i < columns.size() - 1) {
                sql.append(",");
            }
            sql.append(NEW_LINE);
        }
        // 主键约束（BigQuery 支持但有限）
        if (!table.getPrimaryKeys().isEmpty()) {
            sql.append(COMMA).append(NEW_LINE);
            sql.append(INDENT).append(buildPrimaryKey(table, table.getPrimaryKeys().get(0)));
        }
        sql.append(")");
        // 表选项
        appendTableOptions(sql, table);
        return sql.toString();
    }

    /**
     * 构建创建外部表语句
     */
    public String buildCreateExternalTable(JQuickTableDefinition table, String uri, String format, Map<String, String> options) {
        if (table.getExtensions() == null) {
            table.setExtensions(new HashMap<>());
        }
        table.getExtensions().put("tableType", "EXTERNAL");
        table.getExtensions().put("externalTableUri", uri);
        table.getExtensions().put("externalTableFormat", format);
        if (options != null) {
            for (Map.Entry<String, String> entry : options.entrySet()) {
                table.getExtensions().put(entry.getKey(), entry.getValue());
            }
        }
        return buildCreateTable(table);
    }

    /**
     * 构建创建快照表语句
     */
    public String buildCreateSnapshotTable(String snapshotName, String sourceTable) {
        String sb = "CREATE SNAPSHOT TABLE " + quoteIdentifier(null, snapshotName) +
                " CLONE " + quoteIdentifier(null, sourceTable);
        return sb;
    }

    /**
     * 构建创建物化视图语句
     */
    public String buildCreateMaterializedView(String viewName, String query, Map<String, String> options) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE MATERIALIZED VIEW ").append(quoteIdentifier(null, viewName));
        if (options != null && !options.isEmpty()) {
            sb.append(" (\n");
            boolean first = true;
            for (Map.Entry<String, String> entry : options.entrySet()) {
                if (!first) sb.append(",\n");
                sb.append(INDENT).append(entry.getKey()).append(" ").append(entry.getValue());
                first = false;
            }
            sb.append("\n)");
        }
        sb.append(" AS\n").append(query);
        return sb.toString();
    }

    /**
     * 构建刷新物化视图语句
     */
    public String buildRefreshMaterializedView(String viewName) {
        return "REFRESH MATERIALIZED VIEW " + quoteIdentifier(null, viewName);
    }

    @Override
    public String buildColumnDefinition(JQuickTableDefinition table, JQuickColumnDefinition column) {
        StringBuilder def = new StringBuilder();
        def.append(quoteIdentifier(table, column.getColumnName())).append(SPACE);
        def.append(getDataTypeString(table, column.getDataType()));
        if (!column.isNullable()) {
            def.append(" NOT NULL");
        }
        return def.toString();
    }

    @Override
    public String buildPrimaryKey(JQuickTableDefinition table, JQuickPrimaryKeyConstraint pk) {
        // BigQuery 支持主键但为非强制
        String sb = "PRIMARY KEY (" +
                formatColumnList(table, pk.getColumns()) +
                ")" +
                " NOT ENFORCED";
        return sb;
    }

    @Override
    public String buildUniqueConstraint(JQuickTableDefinition table, JQuickUniqueConstraint uc) {
        // BigQuery 支持唯一约束但为非强制
        String sb = "UNIQUE (" +
                formatColumnList(table, uc.getColumns()) +
                ")" +
                " NOT ENFORCED";
        return sb;
    }

    @Override
    public String buildForeignKey(JQuickTableDefinition table, JQuickForeignKeyConstraint fk) {
        // BigQuery 支持外键但为非强制
        String sb = "FOREIGN KEY (" +
                formatColumnList(table, fk.getColumns()) +
                ") REFERENCES " + quoteIdentifier(table, fk.getReferencedTable()) +
                " (" + formatColumnList(table, fk.getReferencedColumns()) + ")" +
                " NOT ENFORCED";
        return sb;
    }

    @Override
    public String buildIndex(JQuickTableDefinition table, JQuickIndexDefinition index) {
        // BigQuery 支持搜索索引
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE ");
        if (index.isUnique()) {
            sb.append("UNIQUE ");
        }
        sb.append("SEARCH INDEX ").append(quoteIdentifier(table, index.getIndexName()));
        sb.append(" ON ").append("${tableName}").append(" (");
        sb.append(formatColumnList(table, index.getColumns()));
        sb.append(")");
        return sb.toString();
    }

    @Override
    public String buildModifyColumn(JQuickTableDefinition table, String tableName, JQuickColumnDefinition column) {
        // BigQuery 使用 ALTER TABLE 修改列
        String sb = "ALTER TABLE " + quoteIdentifier(table, tableName) +
                " ALTER COLUMN " + quoteIdentifier(table, column.getColumnName()) +
                " SET DATA TYPE " + getDataTypeString(table, column.getDataType());
        return sb;
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
        sb.append(" SET DATA TYPE ").append(getDataTypeString(table, newColumn.getDataType()));
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

        return sb.toString();
    }

    @Override
    public String buildDropColumn(JQuickTableDefinition table, String tableName, String columnName) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " DROP COLUMN " + quoteIdentifier(table, columnName);
    }

    @Override
    public String buildDropTable(JQuickTableDefinition table, String tableName, boolean ifExists) {
        StringBuilder sb = new StringBuilder("DROP TABLE ");
        if (ifExists) {
            sb.append("IF EXISTS ");
        }
        sb.append(quoteIdentifier(table, tableName));
        return sb.toString();
    }

    @Override
    public String buildTruncateTable(JQuickTableDefinition table, String tableName) {
        return "TRUNCATE TABLE " + quoteIdentifier(table, tableName);
    }

    @Override
    public String buildRenameTable(JQuickTableDefinition table, String oldName, String newName) {
        return "ALTER TABLE " + quoteIdentifier(table, oldName) + " RENAME TO " + quoteIdentifier(table, newName);
    }

    @Override
    public String buildShowCreateTable(JQuickTableDefinition table, String tableName) {
        return "SELECT * FROM " + quoteIdentifier(table, tableName) + ".INFORMATION_SCHEMA.TABLES";
    }

    @Override
    public String buildDescribeTable(JQuickTableDefinition table, String tableName) {
        return "SELECT * FROM " + quoteIdentifier(table, tableName) + ".INFORMATION_SCHEMA.COLUMNS";
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

    /**
     * 构建覆盖写入语句
     */
    public String buildInsertOverwrite(JQuickTableDefinition table, String query) {
        return "INSERT OR REPLACE INTO " + quoteIdentifier(table, table.getTableName()) + "\n" + query;
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
        sb.append(" FROM ");
        // 通配符表查询
        if (table.getExtensions() != null && table.getExtensions().containsKey("wildcardTable")) {
            String wildcard = table.getExtensions().get("wildcardTable").toString();
            sb.append(quoteIdentifier(table, wildcard));
        } else {
            sb.append(quoteIdentifier(table, table.getTableName()));
        }
        if (whereClause != null && !whereClause.trim().isEmpty()) {
            sb.append(" WHERE ").append(whereClause);
        }

        return sb.toString();
    }

    /**
     * 构建时间分区配置
     */
    public String buildTimePartition(String column, String type, Integer expirationDays) {
        StringBuilder sb = new StringBuilder();
        sb.append("PARTITION BY ").append(column);
        if (type != null) {
            sb.append(" (").append(type).append(")");
        }
        if (expirationDays != null && expirationDays > 0) {
            sb.append("\nOPTIONS (partition_expiration_days = ").append(expirationDays).append(")");
        }

        return sb.toString();
    }

    /**
     * 构建范围分区配置
     */
    public String buildRangePartition(String column, int start, int end, int interval) {
        return "PARTITION BY RANGE_BUCKET(" + column + ", GENERATE_ARRAY(" + start + ", " + end + ", " + interval + "))";
    }

    /**
     * 构建聚簇配置
     */
    public String buildClusterBy(List<String> columns) {
        return "CLUSTER BY " + columns.stream().collect(Collectors.joining(", "));
    }

    /**
     * 构建通配符表查询
     */
    public String buildWildcardQuery(String tablePrefix, String whereClause) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT * FROM `").append(tablePrefix).append("*`");

        if (whereClause != null && !whereClause.isEmpty()) {
            sb.append(" WHERE ").append(whereClause);
        }

        return sb.toString();
    }

    /**
     * 构建表后缀过滤查询
     */
    public String buildTableSuffixFilterQuery(String tablePrefix, String tableSuffix, String whereClause) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT * FROM `").append(tablePrefix).append("*`");
        sb.append(" WHERE _TABLE_SUFFIX = '").append(tableSuffix).append("'");

        if (whereClause != null && !whereClause.isEmpty()) {
            sb.append(" AND ").append(whereClause);
        }

        return sb.toString();
    }

    /**
     * 构建创建存储过程语句
     */
    public String buildCreateProcedure(String procedureName, List<String> parameters, String body) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE OR REPLACE PROCEDURE ").append(procedureName);
        sb.append("(");
        if (parameters != null && !parameters.isEmpty()) {
            sb.append(String.join(", ", parameters));
        }
        sb.append(")\n");
        sb.append("BEGIN\n");
        sb.append(body);
        sb.append("\nEND");
        return sb.toString();
    }

    /**
     * 构建调用存储过程语句
     */
    public String buildCallProcedure(String procedureName, List<String> args) {
        StringBuilder sb = new StringBuilder();
        sb.append("CALL ").append(procedureName);
        sb.append("(");
        if (args != null && !args.isEmpty()) {
            sb.append(String.join(", ", args));
        }
        sb.append(")");
        return sb.toString();
    }

    /**
     * 构建变量声明语句
     */
    public String buildDeclareVariable(String varName, String dataType, Object defaultValue) {
        StringBuilder sb = new StringBuilder();
        sb.append("DECLARE ").append(varName).append(" ").append(dataType);
        if (defaultValue != null) {
            sb.append(" DEFAULT ").append(formatValue(defaultValue));
        }
        return sb.toString();
    }

    /**
     * 构建变量赋值语句
     */
    public String buildSetVariable(String varName, String expression) {
        return "SET " + varName + " = " + expression;
    }
    /**
     * 构建 IF 语句
     */
    public String buildIfStatement(String condition, String thenBody, String elseBody) {
        StringBuilder sb = new StringBuilder();
        sb.append("IF ").append(condition).append(" THEN\n");
        sb.append(thenBody);
        if (elseBody != null && !elseBody.isEmpty()) {
            sb.append("\nELSE\n");
            sb.append(elseBody);
        }
        sb.append("\nEND IF");
        return sb.toString();
    }

    /**
     * 构建循环语句
     */
    public String buildLoopStatement(String loopVariable, String startValue, String endValue, String body) {
        String sb = "DECLARE " + loopVariable + " INT64 DEFAULT " + startValue + ";\n" +
                "WHILE " + loopVariable + " <= " + endValue + " DO\n" +
                body +
                "\n  SET " + loopVariable + " = " + loopVariable + " + 1;\n" +
                "END WHILE";
        return sb;
    }

    /**
     * 构建创建数据集语句
     */
    public String buildCreateDataset(String datasetName, String location, Map<String, String> labels) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE SCHEMA IF NOT EXISTS ").append(quoteIdentifier(null, datasetName));
        if (location != null && !location.isEmpty()) {
            sb.append("\nOPTIONS (");
            sb.append("location = '").append(location).append("'");

            if (labels != null && !labels.isEmpty()) {
                sb.append(",\n  labels = [");
                boolean first = true;
                for (Map.Entry<String, String> entry : labels.entrySet()) {
                    if (!first) sb.append(", ");
                    sb.append("('").append(entry.getKey()).append("', '")
                            .append(escapeString(entry.getValue())).append("')");
                    first = false;
                }
                sb.append("]");
            }
            sb.append(")");
        }

        return sb.toString();
    }

    /**
     * 构建删除数据集语句
     */
    public String buildDropDataset(String datasetName, boolean ifExists, boolean cascade) {
        StringBuilder sb = new StringBuilder("DROP SCHEMA ");
        if (ifExists) {
            sb.append("IF EXISTS ");
        }
        sb.append(quoteIdentifier(null, datasetName));
        if (cascade) {
            sb.append(" CASCADE");
        }
        return sb.toString();
    }

    /**
     * 构建显示数据集语句
     */
    public String buildShowDatasets() {
        return "SELECT schema_name FROM `region-us`.INFORMATION_SCHEMA.SCHEMATA";
    }
    /**
     * 构建复制表语句
     */
    public String buildCopyTable(String sourceTable, String destinationTable) {
        return "CREATE OR REPLACE TABLE " + quoteIdentifier(null, destinationTable) + " CLONE " + quoteIdentifier(null, sourceTable);
    }

    /**
     * 构建导出数据语句
     */
    public String buildExportData(String tableName, String uri, String format) {
        String sb = "EXPORT DATA OPTIONS(\n" +
                "  uri = '" + uri + "',\n" +
                "  format = '" + format.toUpperCase() + "',\n" +
                "  overwrite = TRUE\n" +
                ") AS\n" +
                "SELECT * FROM " + quoteIdentifier(null, tableName);
        return sb;
    }

    /**
     * 构建加载数据语句
     */
    public String buildLoadData(String tableName, String uri, String format, Map<String, String> options) {
        StringBuilder sb = new StringBuilder();
        sb.append("LOAD DATA INTO ").append(quoteIdentifier(null, tableName));
        sb.append("\nFROM FILES (\n");
        sb.append("  uris = ['").append(uri).append("'],\n");
        sb.append("  format = '").append(format.toUpperCase()).append("'");
        if (options != null) {
            for (Map.Entry<String, String> entry : options.entrySet()) {
                sb.append(",\n  ").append(entry.getKey()).append(" = '")
                        .append(escapeString(entry.getValue())).append("'");
            }
        }

        sb.append("\n)");
        return sb.toString();
    }


    /**
     * 构建查询作业语句
     */
    public String buildShowJobs() {
        return "SELECT * FROM `region-us`.INFORMATION_SCHEMA.JOBS ORDER BY creation_time DESC LIMIT 100";
    }

    /**
     * 构建取消作业语句
     */
    public String buildCancelJob(String jobId) {
        return "CANCEL JOB `" + jobId + "`";
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
        return "EXPLAIN ANALYZE " + query;
    }

    /**
     * 构建查询优化提示
     */
    public String buildQueryHint(String query, String hint) {
        return "SELECT /*+ " + hint + " */ " + query;
    }


    private boolean isTemporary(JQuickTableDefinition table) {
        return table.getExtensions() != null &&
                table.getExtensions().containsKey("temporary") &&
                (Boolean) table.getExtensions().get("temporary");
    }

    private boolean ifNotExists(JQuickTableDefinition table) {
        return table.getExtensions() != null &&
                table.getExtensions().containsKey("ifNotExists") &&
                (Boolean) table.getExtensions().get("ifNotExists");
    }

    @Override
    protected void appendNullClause(StringBuilder def, JQuickColumnDefinition column) {
        // 已在列定义中处理
    }

    @Override
    protected void appendDefaultClause(StringBuilder def, JQuickColumnDefinition column) {
        // BigQuery 不支持 DEFAULT 值
    }

    @Override
    protected void appendColumnComment(StringBuilder def, JQuickColumnDefinition column) {
        // 已在列定义中处理
    }

    @Override
    protected String getSpecialDefaultKeyword(String value) {
        // BigQuery 不支持 DEFAULT
        return null;
    }

    @Override
    protected String formatBooleanDefault(boolean value) {
        return value ? "TRUE" : "FALSE";
    }

    @Override
    protected String convertForeignKeyAction(JQuickForeignKeyAction action) {
        return "NO ACTION";
    }

}
