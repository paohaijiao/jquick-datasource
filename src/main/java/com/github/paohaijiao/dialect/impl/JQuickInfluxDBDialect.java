package com.github.paohaijiao.dialect.impl;


import com.github.paohaijiao.column.JQuickColumnDefinition;
import com.github.paohaijiao.connector.JQuickDataSourceConnector;
import com.github.paohaijiao.dataType.JQuickDataTypeConverter;
import com.github.paohaijiao.dataType.impl.JQuickInfluxDBDataTypeConverter;
import com.github.paohaijiao.dialect.JQuickAbsSQLDialect;
import com.github.paohaijiao.row.JQuickRow;
import com.github.paohaijiao.table.JQuickTableDefinition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * InfluxDB 方言实现
 * 时序数据库，使用 InfluxQL
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/15
 */
public class JQuickInfluxDBDialect extends JQuickAbsSQLDialect {

    protected static final String INFLUXDB_QUOTE = "\"";

    @Override
    protected JQuickDataTypeConverter createDataTypeConvert() {
        return new JQuickInfluxDBDataTypeConverter();
    }

    @Override
    protected String getQuoteKeyWord() {
        return INFLUXDB_QUOTE;
    }

    @Override
    public String getAutoIncrementKeyword(JQuickTableDefinition tableDefinition) {
        return "";
    }

    @Override
    protected void appendTableOptions(StringBuilder sql, JQuickTableDefinition table) {
        if (table.getExtensions() != null) {
            // 保留策略
            if (table.getExtensions().containsKey("retentionPolicy")) {
                String retention = table.getExtensions().get("retentionPolicy").toString();
                sql.append(" RETENTION POLICY ").append(retention);
            }
            // 分片持续时间
            if (table.getExtensions().containsKey("shardDuration")) {
                String shardDuration = table.getExtensions().get("shardDuration").toString();
                sql.append(" SHARD DURATION ").append(shardDuration);
            }
            // 副本因子
            if (table.getExtensions().containsKey("replicationFactor")) {
                int replication = (Integer) table.getExtensions().get("replicationFactor");
                sql.append(" REPLICATION ").append(replication);
            }
        }
    }

    @Override
    public String getDriverClass(JQuickDataSourceConnector connector) {
        if (connector != null && connector.getDriverClass() != null && !connector.getDriverClass().trim().isEmpty()) {
            return connector.getDriverClass();
        }
        return "net.suteren.jdbc.influxdb.InfluxDbDriver";
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
            throw new IllegalStateException("Host is required for InfluxDB connection");
        }
        String effectivePort = (port != null && !port.trim().isEmpty()) ? port : "8086";
        StringBuilder url = new StringBuilder();
        url.append("jdbc:influxdb://").append(host).append(":").append(effectivePort);
        if (database != null && !database.trim().isEmpty()) {
            url.append("/").append(database);
        }
        boolean hasParams = false;
        if (username != null && !username.trim().isEmpty()) {
            url.append(hasParams ? "&" : "?").append("user=").append(username);
            hasParams = true;
        }
        if (password != null && !password.trim().isEmpty()) {
            url.append(hasParams ? "&" : "?").append("password=").append(password);
            hasParams = true;
        }

        return url.toString();
    }

    @Override
    public String buildCreateTable(JQuickTableDefinition table) {
        // InfluxDB 不需要预先创建表，这里实现创建连续查询
        StringBuilder sql = new StringBuilder();
        if (table.getExtensions() != null && table.getExtensions().containsKey("continuousQuery")) {
            sql.append("CREATE CONTINUOUS QUERY ").append(quoteIdentifier(table, table.getTableName()));
            sql.append(" ON ").append(table.getExtensions().get("database"));
            sql.append(" BEGIN\n");
            sql.append("  ").append(table.getExtensions().get("query"));
            sql.append("\nEND");
        } else {
            sql.append("-- InfluxDB uses implicit schema. Use INSERT to create measurements.");
        }
        return sql.toString();
    }

    /**
     * 创建保留策略
     */
    public String buildCreateRetentionPolicy(String policyName, String database, String duration, int replication) {
        return "CREATE RETENTION POLICY \"" + policyName + "\" ON \"" + database + "\" DURATION " + duration + " REPLICATION " + replication + " DEFAULT";
    }

    /**
     * 创建连续查询
     */
    public String buildCreateContinuousQuery(String queryName, String database, String selectQuery, String intoMeasurement) {
        return "CREATE CONTINUOUS QUERY \"" + queryName + "\" ON \"" + database + "\" BEGIN\n  " + selectQuery + " INTO \"" + intoMeasurement + "\" GROUP BY time(1h)\nEND";
    }

    /**
     * 构建时间范围查询
     */
    public String buildTimeRangeQuery(String measurement, List<String> fields, String timeRange, String groupByTime) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        if (fields == null || fields.isEmpty()) {
            sb.append("*");
        } else {
            sb.append(String.join(", ", fields));
        }
        sb.append(" FROM \"").append(measurement).append("\"");
        sb.append(" WHERE ").append(timeRange);
        if (groupByTime != null && !groupByTime.isEmpty()) {
            sb.append(" GROUP BY time(").append(groupByTime).append(")");
        }
        return sb.toString();
    }

    @Override
    public String buildInsert(JQuickTableDefinition table, JQuickRow row) {
        // InfluxDB 行协议格式
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(quoteIdentifier(table, table.getTableName()));
        // 标签和字段分离
        List<String> tags = new ArrayList<>();
        List<String> fields = new ArrayList<>();
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            if (entry.getKey().startsWith("tag_")) {
                tags.add(entry.getKey().substring(4) + "=" + formatValueForTag(entry.getValue()));
            } else {
                fields.add(entry.getKey() + "=" + formatValueForField(entry.getValue()));
            }
        }
        if (!tags.isEmpty()) {
            sb.append(",").append(String.join(",", tags));
        }
        sb.append(" ");
        sb.append(String.join(",", fields));

        // 时间戳
        if (row.containsKey("timestamp")) {
            sb.append(" ").append(row.get("timestamp"));
        }

        return sb.toString();
    }

    private String formatValueForTag(Object value) {
        if (value == null) return "";
        return escapeStringForTag(value.toString());
    }

    private String formatValueForField(Object value) {
        if (value == null) return "null";
        if (value instanceof Number) return value.toString();
        if (value instanceof Boolean) return value.toString();
        return "\"" + escapeStringForField(value.toString()) + "\"";
    }

    private String escapeStringForTag(String s) {
        return s.replace("\\", "\\\\").replace(",", "\\,").replace(" ", "\\ ").replace("=", "\\=");
    }

    private String escapeStringForField(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"");
    }

    @Override
    public String buildSelect(JQuickTableDefinition table, List<String> columns, String whereClause) {
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        if (columns == null || columns.isEmpty()) {
            sb.append("*");
        } else {
            sb.append(String.join(", ", columns));
        }
        sb.append(" FROM \"").append(table.getTableName()).append("\"");
        if (whereClause != null && !whereClause.trim().isEmpty()) {
            sb.append(" WHERE ").append(whereClause);
        }
        return sb.toString();
    }

    @Override
    public String buildDelete(JQuickTableDefinition table, String whereClause) {
        // InfluxDB 使用 DELETE 语句
        StringBuilder sb = new StringBuilder();
        sb.append("DELETE FROM \"").append(table.getTableName()).append("\"");
        if (whereClause != null && !whereClause.trim().isEmpty()) {
            sb.append(" WHERE ").append(whereClause);
        }
        return sb.toString();
    }

    // InfluxDB 不需要的方法
    @Override
    protected void appendNullClause(StringBuilder def, JQuickColumnDefinition column) {
    }

    @Override
    protected void appendDefaultClause(StringBuilder def, JQuickColumnDefinition column) {
    }

    @Override
    protected void appendColumnComment(StringBuilder def, JQuickColumnDefinition column) {
    }

    @Override
    protected String getSpecialDefaultKeyword(String value) {
        return null;
    }

    @Override
    protected String formatBooleanDefault(boolean value) {
        return value ? "true" : "false";
    }

    @Override
    protected String convertForeignKeyAction(com.github.paohaijiao.enums.JQuickForeignKeyAction action) {
        return "";
    }

    @Override
    public String buildColumnDefinition(JQuickTableDefinition table, JQuickColumnDefinition column) {
        return "";
    }

    @Override
    public String buildPrimaryKey(JQuickTableDefinition table, com.github.paohaijiao.extra.JQuickPrimaryKeyConstraint pk) {
        return "";
    }

    @Override
    public String buildUniqueConstraint(JQuickTableDefinition table, com.github.paohaijiao.extra.JQuickUniqueConstraint uc) {
        return "";
    }

    @Override
    public String buildForeignKey(JQuickTableDefinition table, com.github.paohaijiao.extra.JQuickForeignKeyConstraint fk) {
        return "";
    }

    @Override
    public String buildModifyColumn(JQuickTableDefinition table, String tableName, JQuickColumnDefinition column) {
        return "";
    }

    @Override
    public String buildChangeColumn(JQuickTableDefinition table, String tableName, String oldName, JQuickColumnDefinition column) {
        return "";
    }

    @Override
    public String buildShowCreateTable(JQuickTableDefinition table, String tableName) {
        return "SHOW SERIES FROM " + tableName;
    }

    @Override
    public String buildDescribeTable(JQuickTableDefinition table, String tableName) {
        return "SHOW FIELD KEYS FROM " + quoteIdentifier(table, tableName);
    }

    @Override
    public String buildUpdate(JQuickTableDefinition table, JQuickRow row, String whereClause) {
        return "";
    }

    @Override
    public String buildIndex(JQuickTableDefinition table, com.github.paohaijiao.extra.JQuickIndexDefinition index) {
        return "";
    }
}
