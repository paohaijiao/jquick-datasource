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
package com.github.paohaijiao.dialect.impl;

import com.github.paohaijiao.column.JQuickColumnDefinition;
import com.github.paohaijiao.connector.JQuickDataSourceConnector;
import com.github.paohaijiao.dataType.JQuickDataTypeConverter;
import com.github.paohaijiao.dataType.impl.JQuickMySQLDataTypeConverter;
import com.github.paohaijiao.dialect.JQuickAbsSQLDialect;
import com.github.paohaijiao.dialect.JQuickSQLDialect;
import com.github.paohaijiao.extra.JQuickIndexDefinition;
import com.github.paohaijiao.row.JQuickRow;
import com.github.paohaijiao.table.JQuickTableDefinition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * SQL方言抽象基类
 * 提供公共的流程和方法实现，子类可覆盖特定方法以适应不同数据库
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/13
 */
public  class JQuickMySQLDialect extends JQuickAbsSQLDialect implements JQuickSQLDialect {

    protected static final String MYSQL_QUOTE = "`";



    @Override
    public String getAutoIncrementKeyword(JQuickTableDefinition table) {
        return "AUTO_INCREMENT";
    }

    @Override
    protected JQuickDataTypeConverter createDataTypeConvert() {
        return new JQuickMySQLDataTypeConverter();
    }

    @Override
    protected String getQuoteKeyWord() {
        return MYSQL_QUOTE;
    }



    @Override
    protected void appendTableOptions(StringBuilder sql, JQuickTableDefinition table) {
        if (table.getComment() != null && !table.getComment().isEmpty()) {
            sql.append(" COMMENT = '").append(escapeString(table.getComment())).append("'");
        }
        if (table.getExtensions() != null && !table.getExtensions().isEmpty()) {
            for (Map.Entry<String, Object> entry : table.getExtensions().entrySet()) {
                if ( entry.getKey() == null) {
                    continue;
                }
                String key = entry.getKey().toLowerCase();
                Object value = entry.getValue();
                if (value == null) {
                    continue;
                }
                sql.append(" ").append(key).append("=").append(value);
            }
        }
    }

    @Override
    public String getDriverClass(JQuickDataSourceConnector connector) {
        if (connector != null && connector.getDriverClass() != null && !connector.getDriverClass().trim().isEmpty()) {
            return connector.getDriverClass();
        }
        return "com.mysql.cj.jdbc.Driver";
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
            throw new IllegalStateException("Host is required for MySQL connection");
        }
        String effectivePort = (port != null && !port.trim().isEmpty()) ? port : "3306";
        StringBuilder url = new StringBuilder();
        url.append("jdbc:mysql://").append(host).append(":").append(effectivePort);
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
        if (hasParams) {
            url.append("&useSSL=false");
            url.append("&serverTimezone=UTC");
            url.append("&allowPublicKeyRetrieval=true");
            url.append("&characterEncoding=UTF-8");
        } else {
            url.append("?useSSL=false");
            url.append("&serverTimezone=UTC");
            url.append("&allowPublicKeyRetrieval=true");
            url.append("&characterEncoding=UTF-8");
        }
        return url.toString();
    }


    @Override
    public String buildIndex(JQuickTableDefinition table,JQuickIndexDefinition index) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE ");
        if (index.isUnique()) {
            sb.append("UNIQUE ");
        }
        sb.append("INDEX ").append(quoteIdentifier(table,index.getIndexName())).append(" ");
        if (index.getType() != null) {
            switch (index.getType()) {
                case "BTREE":
                    sb.append("USING BTREE ");
                    break;
                case "HASH":
                    sb.append("USING HASH ");
                    break;
                case "FULLTEXT":
                    sb.append("FULLTEXT ");
                    break;
                default:
                    break;
            }
        }

        sb.append("ON ").append("${tableName}").append(" (");
        sb.append(formatColumnList(table,index.getColumns()));
        sb.append(")");
        if (index.getComment() != null && !index.getComment().isEmpty()) {
            sb.append(" COMMENT '").append(escapeString(index.getComment())).append("'");
        }

        return sb.toString();
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
        if ("CURRENT_TIME".equals(upperValue)) {
            return "CURRENT_TIME";
        }
        if ("CURRENT_TIMESTAMP(6)".equals(upperValue)) {
            return "CURRENT_TIMESTAMP(6)";
        }
        return null;
    }

    /**
     * 构建 ALTER TABLE MODIFY COLUMN 语句
     */
    public String buildModifyColumn(JQuickTableDefinition table,String tableName, JQuickColumnDefinition column) {
        return "ALTER TABLE " + quoteIdentifier(table,tableName) + " MODIFY " + buildColumnDefinition(table,column);
    }

    /**
     * 构建 ALTER TABLE CHANGE COLUMN 语句（重命名列）
     */
    public String buildChangeColumn(JQuickTableDefinition table,String tableName, String oldName, JQuickColumnDefinition newColumn) {
        return "ALTER TABLE " + quoteIdentifier(table,tableName) + " CHANGE " + quoteIdentifier(table,oldName) + " " + buildColumnDefinition(table,newColumn);
    }

    /**
     * 构建 SHOW CREATE TABLE 语句
     */
    public String buildShowCreateTable(JQuickTableDefinition table,String tableName) {
        return "SHOW CREATE TABLE " + quoteIdentifier(table,tableName);
    }

    /**
     * 构建 DESCRIBE 语句
     */
    public String buildDescribeTable(JQuickTableDefinition table,String tableName) {
        return "DESCRIBE " + quoteIdentifier(table,tableName);
    }

    @Override
    public String buildInsert(JQuickTableDefinition table,JQuickRow row ) {
        if (row == null || row.isEmpty() || table == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(quoteIdentifier(table,table.getTableName())).append(" (");
        List<String> columns = new ArrayList<>(row.keySet());
        sb.append(columns.stream().map(e->quoteIdentifier(table,e)).collect(Collectors.joining(", ")));
        sb.append(") VALUES (");
        List<String> values = new ArrayList<>();
        for (String col : columns) {
            Object value = row.get(col);
            values.add(formatValue(value));
        }
        sb.append(String.join(", ", values));
        sb.append(")");
        return sb.toString();
    }

    @Override
    public String buildUpdate(JQuickTableDefinition table,JQuickRow row,  String whereClause) {
        if (row == null || row.isEmpty() || table == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE ").append(quoteIdentifier(table,table.getTableName())).append(" SET ");
        List<String> setClauses = new ArrayList<>();
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            setClauses.add(quoteIdentifier(table,entry.getKey()) + " = " + formatValue(entry.getValue()));
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
        sb.append("DELETE FROM ").append(quoteIdentifier(table,table.getTableName()));
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
            sb.append(columns.stream().map(e->quoteIdentifier(table,e)).collect(Collectors.joining(", ")));
        }
        sb.append(" FROM ").append(quoteIdentifier(table,table.getTableName()));
        if (whereClause != null && !whereClause.trim().isEmpty()) {
            sb.append(" WHERE ").append(whereClause);
        }
        return sb.toString();
    }
}
