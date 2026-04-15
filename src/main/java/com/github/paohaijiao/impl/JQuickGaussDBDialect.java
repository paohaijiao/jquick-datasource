package com.github.paohaijiao.impl;

import com.github.paohaijiao.table.JQuickTableDefinition;

import java.util.List;

public class JQuickGaussDBDialect extends JQuickPgESDialect {

    @Override
    protected void appendTableOptions(StringBuilder sql, JQuickTableDefinition table) {
        super.appendTableOptions(sql, table);
        // GaussDB 特有选项
        if (table.getExtensions() != null) {
            // 分布策略
            if (table.getExtensions().containsKey("distributeBy")) {
                String distributeBy = table.getExtensions().get("distributeBy").toString();
                sql.append(" DISTRIBUTE BY ").append(distributeBy);
            }
            // 节点组
            if (table.getExtensions().containsKey("nodeGroup")) {
                String nodeGroup = table.getExtensions().get("nodeGroup").toString();
                sql.append(" NODE GROUP ").append(nodeGroup);
            }

            // 表组
            if (table.getExtensions().containsKey("tableGroup")) {
                String tableGroup = table.getExtensions().get("tableGroup").toString();
                sql.append(" TABLE GROUP ").append(tableGroup);
            }
        }
    }

    /**
     * 构建节点组创建语句
     */
    public String buildCreateNodeGroup(String groupName, List<String> nodes) {
        return "CREATE NODE GROUP " + groupName + " WITH (" + String.join(", ", nodes) + ")";
    }

    /**
     * 构建数据重分布语句
     */
    public String buildRedistributeTable(String tableName) {
        return "ALTER TABLE " + quoteIdentifier(null, tableName) + " REDISTRIBUTE";
    }
}
