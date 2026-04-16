package com.github.paohaijiao.dialect.impl;

import com.github.paohaijiao.column.JQuickColumnDefinition;
import com.github.paohaijiao.connector.JQuickDataSourceConnector;
import com.github.paohaijiao.dataType.JQuickDataTypeConverter;
import com.github.paohaijiao.dataType.impl.JQuickTDengineDataTypeConverter;
import com.github.paohaijiao.dialect.JQuickAbsSQLDialect;
import com.github.paohaijiao.row.JQuickRow;
import com.github.paohaijiao.table.JQuickTableDefinition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * TDengine 方言实现
 * 时序数据库，支持超级表和子表
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/15
 */
public class JQuickTDengineDialect extends JQuickAbsSQLDialect {

    protected static final String TDENGINE_QUOTE = "`";

    @Override
    protected JQuickDataTypeConverter createDataTypeConvert() {
        return new JQuickTDengineDataTypeConverter();
    }

    @Override
    protected String getQuoteKeyWord() {
        return TDENGINE_QUOTE;
    }

    @Override
    public String getAutoIncrementKeyword(JQuickTableDefinition tableDefinition) {
        return "";
    }

    @Override
    protected void appendTableOptions(StringBuilder sql, JQuickTableDefinition table) {
        if (table.getExtensions() != null) {
            // 标签定义（TDengine 特有）
            if (table.getExtensions().containsKey("tags")) {
                @SuppressWarnings("unchecked")
                List<JQuickColumnDefinition> tags = (List<JQuickColumnDefinition>) table.getExtensions().get("tags");
                if (tags != null && !tags.isEmpty()) {
                    sql.append(" TAGS (");
                    for (int i = 0; i < tags.size(); i++) {
                        JQuickColumnDefinition tag = tags.get(i);
                        if (i > 0) sql.append(", ");
                        sql.append(quoteIdentifier(table, tag.getColumnName()))
                                .append(" ").append(getDataTypeString(table, tag.getDataType()));
                    }
                    sql.append(")");
                }
            }
        }
    }

    @Override
    public String getDriverClass(JQuickDataSourceConnector connector) {
        if (connector != null && connector.getDriverClass() != null
                && !connector.getDriverClass().trim().isEmpty()) {
            return connector.getDriverClass();
        }
        String connectionType = connector.getByKeyStr("connectionType"); // native, rest, ws
        boolean isRest = "rest".equalsIgnoreCase(connectionType);
        boolean isWebSocket = "ws".equalsIgnoreCase(connectionType);
        if (isRest) {
            return "com.taosdata.jdbc.rs.RestfulDriver";
        } else if (isWebSocket) {
            return "com.taosdata.jdbc.ws.WebSocketDriver";
        } else {
            return "com.taosdata.jdbc.TSDBDriver";
        }
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
            throw new IllegalStateException("Host is required for TDengine connection");
        }
        String connectionType = connector.getByKeyStr("connectionType");
        boolean isNative = "native".equalsIgnoreCase(connectionType) || connectionType == null;
        boolean isRest = "rest".equalsIgnoreCase(connectionType);
        boolean isWebSocket = "ws".equalsIgnoreCase(connectionType);
        String effectivePort;
        if (port != null && !port.trim().isEmpty()) {
            effectivePort = port;
        } else if (isNative) {
            effectivePort = "6030";
        } else {
            effectivePort = "6041";
        }
        String effectiveDatabase = (database != null && !database.trim().isEmpty()) ? database : "";
        StringBuilder url = new StringBuilder();
        if (isWebSocket) {
            url.append("jdbc:TAOS-WS://").append(host).append(":").append(effectivePort);
        } else if (isRest) {
            url.append("jdbc:TAOS-RS://").append(host).append(":").append(effectivePort);
        } else {
            url.append("jdbc:TAOS://").append(host).append(":").append(effectivePort);
        }
        if (!effectiveDatabase.isEmpty()) {
            url.append("/").append(effectiveDatabase);
        }
        boolean hasParams = false;
        String effectiveUser = (username != null && !username.trim().isEmpty()) ? username : "root";
        url.append("?user=").append(effectiveUser);
        hasParams = true;
        String effectivePassword = (password != null && !password.trim().isEmpty()) ? password : "taosdata";
        url.append("&password=").append(effectivePassword);
        String charset = connector.getByKeyStr("charset");
        if (charset != null && !charset.trim().isEmpty()) {
            url.append("&charset=").append(charset);
        }
        String timezone = connector.getByKeyStr("timezone");
        if (timezone != null && !timezone.trim().isEmpty()) {
            url.append("&timezone=").append(timezone);
        }

        String adapterList = connector.getByKeyStr("adapterList");
        if (adapterList != null && !adapterList.trim().isEmpty()) {
            url.append("&adapterList=").append(adapterList);
        }

        return url.toString();
    }

    @Override
    public String buildCreateTable(JQuickTableDefinition table) {
        boolean isSuperTable = false;
        if (table.getExtensions() != null && table.getExtensions().containsKey("isSuperTable")) {
            isSuperTable = (Boolean) table.getExtensions().get("isSuperTable");
        }

        StringBuilder sql = new StringBuilder();

        if (isSuperTable) {
            sql.append("CREATE STABLE ");
        } else {
            sql.append("CREATE TABLE ");
        }

        if (table.getExtensions() != null && table.getExtensions().containsKey("ifNotExists")) {
            Boolean ifNotExists = (Boolean) table.getExtensions().get("ifNotExists");
            if (ifNotExists != null && ifNotExists) {
                sql.append("IF NOT EXISTS ");
            }
        }

        sql.append(quoteIdentifier(table, table.getTableName()));
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

        appendTableOptions(sql, table);

        return sql.toString();
    }

    /**
     * 创建子表（基于超级表）
     */
    public String buildCreateSubTable(String subTableName, String superTableName, Map<String, String> tags) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(quoteIdentifier(null, subTableName));
        sb.append(" USING ").append(quoteIdentifier(null, superTableName));
        sb.append(" TAGS (");

        boolean first = true;
        for (String value : tags.values()) {
            if (!first) sb.append(", ");
            sb.append("'").append(escapeString(value)).append("'");
            first = false;
        }
        sb.append(")");

        return sb.toString();
    }

    /**
     * 插入时序数据
     */
    public String buildInsertTimeseries(String tableName, List<String> columns, List<List<Object>> rows) {
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(quoteIdentifier(null, tableName)).append(" (");
        sb.append(String.join(", ", columns));
        sb.append(") VALUES ");

        for (int i = 0; i < rows.size(); i++) {
            if (i > 0) sb.append(", ");
            sb.append("(");
            List<String> values = new ArrayList<>();
            for (Object value : rows.get(i)) {
                values.add(formatValue(value));
            }
            sb.append(String.join(", ", values));
            sb.append(")");
        }

        return sb.toString();
    }

    /**
     * 时间窗口聚合查询
     */
    public String buildIntervalQuery(String tableName, String column, String aggregate,
                                     String interval, String where) {
        String sb = "SELECT " + aggregate + "(" + column + ")" +
                " FROM " + quoteIdentifier(null, tableName) +
                " WHERE " + where +
                " INTERVAL(" + interval + ")";
        return sb;
    }

    @Override
    public String buildInsert(JQuickTableDefinition table, JQuickRow row) {
        // TDengine 推荐使用参数化插入
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
        return value ? "1" : "0";
    }

    @Override
    protected String convertForeignKeyAction(com.github.paohaijiao.enums.JQuickForeignKeyAction action) {
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
        return "SHOW CREATE TABLE " + tableName;
    }

    @Override
    public String buildDescribeTable(JQuickTableDefinition table, String tableName) {
        return "DESCRIBE " + quoteIdentifier(table, tableName);
    }

    @Override
    public String buildUpdate(JQuickTableDefinition table, JQuickRow row, String whereClause) {
        return "";
    }

    @Override
    public String buildDelete(JQuickTableDefinition table, String whereClause) {
        return "";
    }

    @Override
    public String buildIndex(JQuickTableDefinition table, com.github.paohaijiao.extra.JQuickIndexDefinition index) {
        return "";
    }
}
