package com.github.paohaijiao.dialect.impl;

import com.github.paohaijiao.column.JQuickColumnDefinition;
import com.github.paohaijiao.connector.JQuickDataSourceConnector;
import com.github.paohaijiao.dataType.JQuickDataTypeConverter;
import com.github.paohaijiao.dataType.impl.JQuickSnowflakeDataTypeConverter;
import com.github.paohaijiao.dialect.JQuickAbsSQLDialect;
import com.github.paohaijiao.enums.JQuickForeignKeyAction;
import com.github.paohaijiao.extra.JQuickForeignKeyConstraint;
import com.github.paohaijiao.extra.JQuickIndexDefinition;
import com.github.paohaijiao.extra.JQuickPrimaryKeyConstraint;
import com.github.paohaijiao.extra.JQuickUniqueConstraint;
import com.github.paohaijiao.row.JQuickRow;
import com.github.paohaijiao.table.JQuickTableDefinition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Snowflake 方言实现
 * 云数据仓库，独立 SQL 方言
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/16
 */
public class JQuickSnowflakeDialect extends JQuickAbsSQLDialect {

    protected static final String SNOWFLAKE_QUOTE = "\"";

    @Override
    protected JQuickDataTypeConverter createDataTypeConvert() {
        return new JQuickSnowflakeDataTypeConverter();
    }

    @Override
    protected String getQuoteKeyWord() {
        return SNOWFLAKE_QUOTE;
    }

    @Override
    public String getAutoIncrementKeyword(JQuickTableDefinition tableDefinition) {
        return "AUTOINCREMENT";
    }

    @Override
    protected void appendTableOptions(StringBuilder sql, JQuickTableDefinition table) {
        if (table.getExtensions() == null) return;
        // 数据聚类
        if (table.getExtensions().containsKey("clusterBy")) {
            @SuppressWarnings("unchecked")
            List<String> clusterColumns = (List<String>) table.getExtensions().get("clusterBy");
            if (clusterColumns != null && !clusterColumns.isEmpty()) {
                sql.append("\nCLUSTER BY (");
                sql.append(clusterColumns.stream()
                        .map(c -> quoteIdentifier(table, c))
                        .collect(Collectors.joining(", ")));
                sql.append(")");
            }
        }

        // 存储类型
        if (table.getExtensions().containsKey("stage")) {
            String stage = table.getExtensions().get("stage").toString();
            sql.append("\nSTAGE = ").append(quoteIdentifier(table, stage));
        }

        // 数据保留时间
        if (table.getExtensions().containsKey("dataRetentionTimeInDays")) {
            int days = (Integer) table.getExtensions().get("dataRetentionTimeInDays");
            sql.append("\nDATA_RETENTION_TIME_IN_DAYS = ").append(days);
        }

        // 表注释
        if (table.getComment() != null && !table.getComment().isEmpty()) {
            sql.append("\nCOMMENT = '").append(escapeString(table.getComment())).append("'");
        }
    }

    @Override
    public String getDriverClass(JQuickDataSourceConnector connector) {
        if (connector != null && connector.getDriverClass() != null && !connector.getDriverClass().trim().isEmpty()) {
            return connector.getDriverClass();
        }
        return "net.snowflake.client.jdbc.SnowflakeDriver";
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
        String database = connector.getSchema();
        String username = connector.getUsername();
        String password = connector.getPassword();
        String warehouse = connector.getByKeyStr("warehouse");
        String schemaName = connector.getByKeyStr("schema");
        if (host == null || host.trim().isEmpty()) {
            throw new IllegalStateException("Account (host) is required for Snowflake connection");
        }
        if (username == null || username.trim().isEmpty()) {
            throw new IllegalStateException("Username is required for Snowflake connection");
        }
        StringBuilder url = new StringBuilder();
        url.append("jdbc:snowflake://").append(host);
        if (!host.contains(".snowflakecomputing.com") && !host.contains("://")) {
            url.append(".snowflakecomputing.com");
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
        if (database != null && !database.trim().isEmpty()) {
            url.append(hasParams ? "&" : "?").append("db=").append(database);
            hasParams = true;
        }
        if (warehouse != null && !warehouse.trim().isEmpty()) {
            url.append(hasParams ? "&" : "?").append("warehouse=").append(warehouse);
            hasParams = true;
        }
        if (schemaName != null && !schemaName.trim().isEmpty()) {
            url.append(hasParams ? "&" : "?").append("schema=").append(schemaName);
            hasParams = true;
        }
        String role = connector.getByKeyStr("role");
        if (role != null && !role.trim().isEmpty()) {
            url.append(hasParams ? "&" : "?").append("role=").append(role);
            hasParams = true;
        }

        return url.toString();
    }

    @Override
    public String buildCreateTable(JQuickTableDefinition table) {
        StringBuilder sql = new StringBuilder();
        // 临时表
        if (isTemporary(table)) {
            sql.append("CREATE TEMPORARY TABLE ");
        } else if (isTransient(table)) {
            sql.append("CREATE TRANSIENT TABLE ");
        } else {
            sql.append("CREATE TABLE ");
        }
        // IF NOT EXISTS
        if (ifNotExists(table)) {
            sql.append("IF NOT EXISTS ");
        }
        sql.append(quoteIdentifier(table, table.getTableName()));
        // 列定义
        sql.append(" (\n");
        List<JQuickColumnDefinition> columns = table.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            JQuickColumnDefinition column = columns.get(i);
            sql.append(INDENT).append(quoteIdentifier(table, column.getColumnName()))
                    .append(" ").append(getDataTypeString(table, column.getDataType()));

            if (!column.isNullable()) {
                sql.append(" NOT NULL");
            }
            if (column.isAutoIncrement()) {
                sql.append(" AUTOINCREMENT");
                if (column.getExtensions() != null && column.getExtensions().containsKey("identityStart")) {
                    sql.append(" START ").append(column.getExtensions().get("identityStart"));
                }
            }
            if (column.getDefaultValue() != null && !column.isAutoIncrement()) {
                sql.append(" DEFAULT ").append(formatDefaultValue(column.getDefaultValue()));
            }
            if (i < columns.size() - 1) {
                sql.append(",");
            }
            sql.append(NEW_LINE);
        }
        // 主键
        if (!table.getPrimaryKeys().isEmpty()) {
            sql.append(COMMA).append(NEW_LINE);
            sql.append(INDENT).append(buildPrimaryKey(table, table.getPrimaryKeys().get(0)));
        }
        sql.append(")");
        appendTableOptions(sql, table);
        return sql.toString();
    }

    /**
     * 构建创建阶段语句
     */
    public String buildCreateStage(String stageName, String url, Map<String, String> credentials) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE STAGE ").append(quoteIdentifier(null, stageName));
        sb.append(" URL = '").append(url).append("'");
        if (credentials != null && !credentials.isEmpty()) {
            sb.append(" CREDENTIALS = (");
            boolean first = true;
            for (Map.Entry<String, String> entry : credentials.entrySet()) {
                if (!first) sb.append(" ");
                sb.append(entry.getKey()).append("='").append(entry.getValue()).append("'");
                first = false;
            }
            sb.append(")");
        }
        return sb.toString();
    }

    /**
     * 构建复制数据语句
     */
    public String buildCopyInto(String tableName, String stageName, String filePattern) {
        StringBuilder sb = new StringBuilder();
        sb.append("COPY INTO ").append(quoteIdentifier(null, tableName));
        sb.append(" FROM @").append(stageName);
        if (filePattern != null) {
            sb.append("/").append(filePattern);
        }
        sb.append("\nFILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY = '\"' SKIP_HEADER = 1)");
        sb.append("\nON_ERROR = 'CONTINUE'");
        return sb.toString();
    }

    /**
     * 构建任务创建语句
     */
    public String buildCreateTask(String taskName, String schedule, String statement) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TASK ").append(quoteIdentifier(null, taskName));
        if (schedule != null) {
            sb.append(" SCHEDULE = '").append(schedule).append("'");
        }
        sb.append("\nAS\n").append(statement);
        return sb.toString();
    }

    /**
     * 构建启动任务语句
     */
    public String buildResumeTask(String taskName) {
        return "ALTER TASK " + quoteIdentifier(null, taskName) + " RESUME";
    }

    /**
     * 构建暂停任务语句
     */
    public String buildSuspendTask(String taskName) {
        return "ALTER TASK " + quoteIdentifier(null, taskName) + " SUSPEND";
    }

    /**
     * 构建查询历史语句
     */
    public String buildQueryHistory() {
        return "SELECT * FROM TABLE(INFORMATION_SCHEMA.QUERY_HISTORY()) ORDER BY START_TIME DESC LIMIT 100";
    }

    /**
     * 构建仓库操作语句
     */
    public String buildCreateWarehouse(String warehouseName, String warehouseSize) {
        return "CREATE WAREHOUSE " + quoteIdentifier(null, warehouseName) + " WAREHOUSE_SIZE = '" + warehouseSize + "' AUTO_SUSPEND = 300 AUTO_RESUME = TRUE";
    }

    /**
     * 构建启动仓库语句
     */
    public String buildResumeWarehouse(String warehouseName) {
        return "ALTER WAREHOUSE " + quoteIdentifier(null, warehouseName) + " RESUME";
    }

    /**
     * 构建暂停仓库语句
     */
    public String buildSuspendWarehouse(String warehouseName) {
        return "ALTER WAREHOUSE " + quoteIdentifier(null, warehouseName) + " SUSPEND";
    }

    @Override
    public String buildInsert(JQuickTableDefinition table, JQuickRow row) {
        if (row == null || row.isEmpty() || table == null) return "";
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

    @Override
    public String buildSelect(JQuickTableDefinition table, List<String> columns, String whereClause) {
        if (table == null) return "";
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        // 采样
        if (table.getExtensions() != null && table.getExtensions().containsKey("sample")) {
            Object sample = table.getExtensions().get("sample");
            sb.append("SAMPLE ").append(sample).append(" ");
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

    private boolean isTemporary(JQuickTableDefinition table) {
        return table.getExtensions() != null && table.getExtensions().containsKey("temporary") && (Boolean) table.getExtensions().get("temporary");
    }

    private boolean isTransient(JQuickTableDefinition table) {
        return table.getExtensions() != null && table.getExtensions().containsKey("transient") && (Boolean) table.getExtensions().get("transient");
    }

    private boolean ifNotExists(JQuickTableDefinition table) {
        return table.getExtensions() != null && table.getExtensions().containsKey("ifNotExists") && (Boolean) table.getExtensions().get("ifNotExists");
    }

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
        if (value == null) return null;
        String upper = value.toUpperCase();
        if ("CURRENT_TIMESTAMP".equals(upper)) return "CURRENT_TIMESTAMP";
        if ("CURRENT_DATE".equals(upper)) return "CURRENT_DATE";
        if ("CURRENT_TIME".equals(upper)) return "CURRENT_TIME";
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

    @Override
    public String buildPrimaryKey(JQuickTableDefinition table, JQuickPrimaryKeyConstraint pk) {
        StringBuilder sb = new StringBuilder();
        if (pk.getConstraintName() != null && !pk.getConstraintName().isEmpty()) {
            sb.append("CONSTRAINT ").append(quoteIdentifier(table, pk.getConstraintName())).append(" ");
        }
        sb.append("PRIMARY KEY (");
        sb.append(formatColumnList(table, pk.getColumns()));
        sb.append(")");
        sb.append(" NOT ENFORCED");
        return sb.toString();
    }

    @Override
    public String buildUniqueConstraint(JQuickTableDefinition table, JQuickUniqueConstraint uc) {
        StringBuilder sb = new StringBuilder();
        if (uc.getConstraintName() != null && !uc.getConstraintName().isEmpty()) {
            sb.append("CONSTRAINT ").append(quoteIdentifier(table, uc.getConstraintName())).append(" ");
        }
        sb.append("UNIQUE (");
        sb.append(formatColumnList(table, uc.getColumns()));
        sb.append(")");
        sb.append(" NOT ENFORCED");
        return sb.toString();
    }

    @Override
    public String buildForeignKey(JQuickTableDefinition table, JQuickForeignKeyConstraint fk) {
        StringBuilder sb = new StringBuilder();
        if (fk.getConstraintName() != null && !fk.getConstraintName().isEmpty()) {
            sb.append("CONSTRAINT ").append(quoteIdentifier(table, fk.getConstraintName())).append(" ");
        }
        sb.append("FOREIGN KEY (");
        sb.append(formatColumnList(table, fk.getColumns()));
        sb.append(") REFERENCES ").append(quoteIdentifier(table, fk.getReferencedTable()));
        sb.append(" (").append(formatColumnList(table, fk.getReferencedColumns())).append(")");
        sb.append(" NOT ENFORCED");
        return sb.toString();
    }

    @Override
    public String buildModifyColumn(JQuickTableDefinition table, String tableName, JQuickColumnDefinition column) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " ALTER COLUMN " + quoteIdentifier(table, column.getColumnName()) + " SET DATA TYPE " + getDataTypeString(table, column.getDataType());
    }

    @Override
    public String buildChangeColumn(JQuickTableDefinition table, String tableName, String oldName, JQuickColumnDefinition newColumn) {
        StringBuilder sb = new StringBuilder();
        if (!oldName.equals(newColumn.getColumnName())) {
            sb.append("ALTER TABLE ").append(quoteIdentifier(table, tableName));
            sb.append(" RENAME COLUMN ").append(quoteIdentifier(table, oldName));
            sb.append(" TO ").append(quoteIdentifier(table, newColumn.getColumnName())).append(";\n");
        }
        sb.append(buildModifyColumn(table, tableName, newColumn));
        return sb.toString();
    }

    @Override
    public String buildShowCreateTable(JQuickTableDefinition table, String tableName) {
        return "SHOW TABLES LIKE '" + tableName + "'";
    }

    @Override
    public String buildDescribeTable(JQuickTableDefinition table, String tableName) {
        return "DESCRIBE TABLE " + quoteIdentifier(table, tableName);
    }

    @Override
    public String buildUpdate(JQuickTableDefinition table, JQuickRow row, String whereClause) {
        if (row == null || row.isEmpty() || table == null) return "";
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE ").append(quoteIdentifier(table, table.getTableName())).append(" SET ");
        List<String> setClauses = new ArrayList<>();
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            setClauses.add(quoteIdentifier(table, entry.getKey()) + " = " + formatValue(entry.getValue()));
        }
        sb.append(String.join(", ", setClauses));
        if (whereClause != null && !whereClause.trim().isEmpty()) sb.append(" WHERE ").append(whereClause);
        return sb.toString();
    }

    @Override
    public String buildDelete(JQuickTableDefinition table, String whereClause) {
        if (table == null) return "";
        StringBuilder sb = new StringBuilder();
        sb.append("DELETE FROM ").append(quoteIdentifier(table, table.getTableName()));
        if (whereClause != null && !whereClause.trim().isEmpty()) sb.append(" WHERE ").append(whereClause);
        return sb.toString();
    }

    @Override
    public String buildIndex(JQuickTableDefinition table, JQuickIndexDefinition index) {
        return "";
    }
}

