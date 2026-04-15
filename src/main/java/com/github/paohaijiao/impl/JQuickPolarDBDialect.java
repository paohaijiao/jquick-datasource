package com.github.paohaijiao.impl;

import com.github.paohaijiao.table.JQuickTableDefinition;

import java.util.List;

/**
 * PolarDB 方言实现
 * 阿里云 PolarDB，兼容 MySQL
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/15
 */
public class JQuickPolarDBDialect extends JQuickMySQLDialect {

    @Override
    protected void appendTableOptions(StringBuilder sql, JQuickTableDefinition table) {
        super.appendTableOptions(sql, table);

        // PolarDB 特有选项
        if (table.getExtensions() != null) {
            // 全局二级索引
            if (table.getExtensions().containsKey("globalIndex")) {
                @SuppressWarnings("unchecked")
                List<String> globalIndex = (List<String>) table.getExtensions().get("globalIndex");
                if (globalIndex != null && !globalIndex.isEmpty()) {
                    sql.append(", GLOBAL INDEX ").append(String.join(", ", globalIndex));
                }
            }

            // 列存索引
            if (table.getExtensions().containsKey("columnarIndex")) {
                Boolean columnarIndex = (Boolean) table.getExtensions().get("columnarIndex");
                if (columnarIndex != null && columnarIndex) {
                    sql.append(", COLUMNAR INDEX");
                }
            }

            // 归档模式
            if (table.getExtensions().containsKey("archiveMode")) {
                Boolean archiveMode = (Boolean) table.getExtensions().get("archiveMode");
                if (archiveMode != null && archiveMode) {
                    sql.append(" ARCHIVE = ON");
                }
            }
        }
    }

    /**
     * 构建列存索引创建语句
     */
    public String buildCreateColumnarIndex(String indexName, String tableName, List<String> columns) {
        return "ALTER TABLE " + quoteIdentifier(null, tableName) +
                " ADD COLUMNAR INDEX " + indexName + " (" +
                String.join(", ", columns) + ")";
    }

    /**
     * 构建并行查询设置
     */
    public String buildSetParallelQuery(int degree) {
        return "SET max_parallel_degree = " + degree;
    }
}

