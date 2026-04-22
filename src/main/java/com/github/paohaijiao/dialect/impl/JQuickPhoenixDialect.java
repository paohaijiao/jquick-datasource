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
import com.github.paohaijiao.dataType.JQuickDataType;
import com.github.paohaijiao.dataType.JQuickDataTypeConverter;
import com.github.paohaijiao.dataType.enums.JQuickDataTypeFamily;
import com.github.paohaijiao.dataType.impl.JQuickPhoenixDataTypeConverter;
import com.github.paohaijiao.dialect.JQuickAbsSQLDialect;
import com.github.paohaijiao.extra.JQuickIndexDefinition;
import com.github.paohaijiao.statement.JQuickRow;
import com.github.paohaijiao.table.JQuickTableDefinition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Apache Phoenix 方言实现
 * Phoenix 是构建在 HBase 之上的 SQL 层
 *
 * 核心特性：
 * 1. UPSERT 代替 INSERT/UPDATE
 * 2. 表名/列名大小写敏感，需要双引号包裹
 * 3. 支持列簇语法 (cf.column)
 * 4. 不支持事务和跨行一致性
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/16
 */
public class JQuickPhoenixDialect extends JQuickAbsSQLDialect {

    protected static final String PHOENIX_QUOTE = "\"";

    private static final String DEFAULT_COLUMN_FAMILY = "0";

    @Override
    protected JQuickDataTypeConverter createDataTypeConvert() {
        return new JQuickPhoenixDataTypeConverter();
    }

    @Override
    protected String getQuoteKeyWord() {
        return PHOENIX_QUOTE;
    }

    @Override
    public String getAutoIncrementKeyword(JQuickTableDefinition tableDefinition) {
        return "DEFAULT NEXT VALUE FOR";
    }

    @Override
    protected void appendTableOptions(StringBuilder sql, JQuickTableDefinition table) {
        if (table.getExtensions() != null) {
            Object saltBuckets = table.getExtensions().get("saltBuckets");
            if (saltBuckets != null) {
                sql.append(" SALT_BUCKETS = ").append(saltBuckets);
            }
            Object compression = table.getExtensions().get("compression");
            if (compression != null) {
                sql.append(" COMPRESSION = '").append(compression).append("'");
            }
            Object ttl = table.getExtensions().get("ttl");
            if (ttl != null) {
                sql.append(" TTL = ").append(ttl);
            }
        }
    }

    @Override
    public String getDriverClass(JQuickDataSourceConnector connector) {
        if (connector != null && connector.getDriverClass() != null && !connector.getDriverClass().trim().isEmpty()) {
            return connector.getDriverClass();
        }
        return "org.apache.phoenix.jdbc.PhoenixDriver";
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
        String database = connector.getSchema();      // 数据库名（可选）
        String username = connector.getUsername();
        String password = connector.getPassword();

        if (host == null || host.trim().isEmpty()) {
            throw new IllegalStateException("Host (ZooKeeper quorum) is required for Phoenix connection");
        }
        String effectivePort = (port != null && !port.trim().isEmpty()) ? port : "2181";
        String connectionType = connector.getByKeyStr("connectionType"); // normal, thin
        boolean isThinDriver = "thin".equalsIgnoreCase(connectionType);
        StringBuilder url = new StringBuilder();

        if (isThinDriver) {
            url.append("jdbc:phoenix:thin:url=http://").append(host).append(":").append(effectivePort);
            if (database != null && !database.trim().isEmpty()) {
                url.append("/").append(database);
            }
            if (username != null && !username.trim().isEmpty()) {
                url.append(";serialization=protobuf");
            }
        } else {
            url.append("jdbc:phoenix:").append(host).append(":").append(effectivePort);
            String hbaseRoot = connector.getByKeyStr("hbaseRoot");
            if (hbaseRoot != null && !hbaseRoot.trim().isEmpty()) {
                url.append(":/").append(hbaseRoot);
            }
            if (database != null && !database.trim().isEmpty()) {
                url.append(":/").append(database);
            }
        }

        return url.toString();
    }

    @Override
    protected void appendColumnPosition(JQuickTableDefinition tableDefinition,StringBuilder def, JQuickColumnDefinition column) {
        // Phoenix 不支持 FIRST/AFTER 语法
        // 列顺序由 HBase 列簇决定，位置语法无效
    }

    @Override
    protected String getSpecialDefaultKeyword(String value) {
        if (value == null) {
            return null;
        }
        String upperValue = value.toUpperCase();
        if ("CURRENT_TIMESTAMP".equals(upperValue) || "NOW()".equals(upperValue)) {
            return "CURRENT_TIMESTAMP()";
        }
        if ("CURRENT_DATE".equals(upperValue)) {
            return "CURRENT_DATE()";
        }
        if ("CURRENT_TIME".equals(upperValue)) {
            return "CURRENT_TIME()";
        }
        return null;
    }

    @Override
    public String buildIndex(JQuickTableDefinition tableDefinition,JQuickIndexDefinition index) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE ");
        if (index.isUnique()) {
            sb.append("UNIQUE ");
        }
        if (index.getType() != null) {
            String type = index.getType().toUpperCase();
            if ("LOCAL".equals(type)) {
                sb.append("LOCAL ");
            }
        }

        sb.append("INDEX ").append(quoteIdentifier(index.getIndexName()));
        sb.append(" ON ").append("${tableName}").append(" (");
        sb.append(formatColumnList(tableDefinition,index.getColumns()));
        sb.append(")");
        if (index.getComment() != null && !index.getComment().isEmpty()) {
            sb.append(" INCLUDE (").append(index.getComment()).append(")");
        }

        return sb.toString();
    }

    @Override
    public String buildInsert( JQuickTableDefinition table,JQuickRow row) {
        if (row == null || row.isEmpty() || table == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("UPSERT INTO ").append(quoteIdentifier(table.getTableName())).append(" (");
        List<String> columns = new ArrayList<>(row.keySet());
        sb.append(columns.stream()
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(", ")));
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
    public String buildUpdate( JQuickTableDefinition table,JQuickRow row, String whereClause) {
        if (row == null || row.isEmpty() || table == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("UPSERT INTO ").append(quoteIdentifier(table.getTableName())).append(" (");

        List<String> columns = new ArrayList<>(row.keySet());
        sb.append(columns.stream()
                .map(this::quoteIdentifier)
                .collect(Collectors.joining(", ")));
        sb.append(") VALUES (");

        List<String> values = new ArrayList<>();
        for (String col : columns) {
            values.add(formatValue(row.get(col)));
        }
        sb.append(String.join(", ", values));
        sb.append(")");

        // Phoenix 不支持 WHERE 子句在 UPSERT 中
        // 过滤条件应在主键值中体现
        return sb.toString();
    }

    @Override
    public String buildDelete(JQuickTableDefinition table, String whereClause) {
        if (table == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("DELETE FROM ").append(quoteIdentifier(table.getTableName()));
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
            sb.append(columns.stream()
                    .map(this::quoteIdentifier)
                    .collect(Collectors.joining(", ")));
        }

        sb.append(" FROM ").append(quoteIdentifier(table.getTableName()));

        if (whereClause != null && !whereClause.trim().isEmpty()) {
            sb.append(" WHERE ").append(whereClause);
        }

        return sb.toString();
    }

    @Override
    public String buildModifyColumn( JQuickTableDefinition table,String tableName, JQuickColumnDefinition column) {

        throw new UnsupportedOperationException("Phoenix does not support ALTER TABLE MODIFY COLUMN. " +
                "Please create a new table and migrate data."
        );
    }

    @Override
    public String buildChangeColumn( JQuickTableDefinition table,String tableName, String oldName, JQuickColumnDefinition newColumn) {
        throw new UnsupportedOperationException(
                "Phoenix does not support ALTER TABLE CHANGE COLUMN (rename column)."
        );
    }

    @Override
    public String buildShowCreateTable( JQuickTableDefinition table,String tableName) {
        return "SELECT TABLE_NAME, COLUMN_FAMILY, COLUMN_NAME, TYPE " +
                "FROM SYSTEM.CATALOG " +
                "WHERE TABLE_NAME = '" + tableName.toUpperCase() + "' " +
                "ORDER BY COLUMN_FAMILY, COLUMN_NAME";
    }

    @Override
    public String buildDescribeTable( JQuickTableDefinition table,String tableName) {
        return "SELECT COLUMN_NAME, TYPE, IS_NULLABLE, COLUMN_FAMILY " +
                "FROM SYSTEM.CATALOG " +
                "WHERE TABLE_NAME = '" + tableName.toUpperCase() + "' " +
                "AND COLUMN_NAME IS NOT NULL";
    }
    /**
     * 构建创建序列语句（Phoenix 自增列需要）
     * @param sequenceName 序列名
     * @param startWith 起始值
     * @param incrementBy 步长
     */
    public String buildCreateSequence(String sequenceName, Integer startWith, Integer incrementBy) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE SEQUENCE ").append(quoteIdentifier(sequenceName));
        if (startWith != null) {
            sb.append(" START WITH ").append(startWith);
        }
        if (incrementBy != null) {
            sb.append(" INCREMENT BY ").append(incrementBy);
        }
        return sb.toString();
    }

    /**
     * 构建删除序列语句
     */
    public String buildDropSequence(String sequenceName) {
        return "DROP SEQUENCE " + quoteIdentifier(sequenceName);
    }

    /**
     * 构建 UPSERT 并返回自增值的语句
     */
    public String buildUpsertWithSequence(String tableName, String sequenceName, List<String> columns, List<String> values) {
        StringBuilder sb = new StringBuilder();
        sb.append("UPSERT INTO ").append(quoteIdentifier(tableName)).append(" (");
        sb.append(String.join(", ", columns.stream()
                .map(this::quoteIdentifier)
                .collect(Collectors.toList())));
        sb.append(") VALUES (");
        sb.append("NEXT VALUE FOR ").append(quoteIdentifier(sequenceName)).append(", ");
        sb.append(String.join(", ", values));
        sb.append(")");
        return sb.toString();
    }

    /**
     * 构建带列簇的列定义
     * @param columnFamily 列簇名（HBase 概念）
     * @param columnName 列名
     */
    public String buildColumnWithFamily(String columnFamily, String columnName) {
        return quoteIdentifier(columnFamily) + "." + quoteIdentifier(columnName);
    }

    protected String quoteIdentifier(String identifier) {
        if (identifier == null || identifier.isEmpty()) {
            return identifier;
        }
        if ((identifier.startsWith("\"") && identifier.endsWith("\"")) ||
                (identifier.startsWith("'") && identifier.endsWith("'"))) {
            return identifier;
        }
        if (identifier.contains(".")) {
            String[] parts = identifier.split("\\.", 2);
            return "\"" + parts[0] + "\".\"" + parts[1] + "\"";
        }
        return "\"" + identifier + "\"";
    }

     String format(Object value) {
        if (value == null) {
            return "NULL";
        }
        if (value instanceof String) {
            return "'" + escapeString((String) value) + "'";
        }
        if (value instanceof Number) {
            return value.toString();
        }
        if (value instanceof Boolean) {
            return (Boolean) value ? "TRUE" : "FALSE";
        }
        if (value instanceof java.util.Date) {
            return "'" + value.toString() + "'";
        }
        return "'" + escapeString(value.toString()) + "'";
    }
}