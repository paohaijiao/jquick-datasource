package com.github.paohaijiao.dialect.impl;


import com.github.paohaijiao.table.JQuickTableDefinition;

import java.util.List;
import java.util.stream.Collectors;

/**
 * CockroachDB 方言实现
 * 分布式 SQL 数据库，兼容 PostgreSQL
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/16
 */
public class JQuickCockroachDBDialect extends JQuickPgESDialect {

    @Override
    protected void appendTableOptions(StringBuilder sql, JQuickTableDefinition table) {
        super.appendTableOptions(sql, table);
        if (table.getExtensions() != null) {
            // 本地性配置
            if (table.getExtensions().containsKey("locality")) {
                sql.append(" LOCALITY ").append(table.getExtensions().get("locality"));
            }
            // 生存时间
            if (table.getExtensions().containsKey("ttl")) {
                String ttl = table.getExtensions().get("ttl").toString();
                sql.append(" WITH (ttl = '").append(ttl).append("')");
            }
        }
    }

    /**
     * 构建设置本地性语句
     */
    public String buildSetLocality(String tableName, String locality) {
        return "ALTER TABLE " + quoteIdentifier(null, tableName) + " SET LOCALITY " + locality;
    }

    /**
     * 构建设置生存时间语句
     */
    public String buildSetTTL(String tableName, String columnName, String interval) {
        return "ALTER TABLE " + quoteIdentifier(null, tableName) +
                " ADD COLUMN " + columnName + " TIMESTAMP WITH TIME ZONE,\n" +
                "ALTER TABLE " + quoteIdentifier(null, tableName) +
                " SET (ttl_expiration_expression = '" + columnName + "', ttl = '" + interval + "')";
    }

    /**
     * 构建导出数据语句
     */
    public String buildExportToFile(String tableName, String filePath) {
        return "EXPORT INTO CSV '" + filePath + "' FROM TABLE " + quoteIdentifier(null, tableName);
    }

    /**
     * 构建导入数据语句
     */
    public String buildImportFromFile(String tableName, String filePath) {
        return "IMPORT INTO " + quoteIdentifier(null, tableName) + " CSV DATA ('" + filePath + "')";
    }

    @Override
    public String buildSelect(JQuickTableDefinition table, List<String> columns, String whereClause) {
        if (table == null) return "";
        StringBuilder sb = new StringBuilder();
        sb.append("SELECT ");
        // 强制索引提示
        if (table.getExtensions() != null && table.getExtensions().containsKey("forceIndex")) {
            String indexName = table.getExtensions().get("forceIndex").toString();
            sb.append("@").append(indexName).append(" ");
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
}
