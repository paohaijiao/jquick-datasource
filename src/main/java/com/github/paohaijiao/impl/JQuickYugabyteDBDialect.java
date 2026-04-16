package com.github.paohaijiao.impl;


import com.github.paohaijiao.table.JQuickTableDefinition;

import java.util.List;
import java.util.stream.Collectors;

/**
 * YugabyteDB 方言实现
 * 分布式 SQL 数据库，兼容 PostgreSQL
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/16
 */
public class JQuickYugabyteDBDialect extends JQuickPgESDialect {

    @Override
    protected void appendTableOptions(StringBuilder sql, JQuickTableDefinition table) {
        super.appendTableOptions(sql, table);

        if (table.getExtensions() != null) {
            // 分片策略
            if (table.getExtensions().containsKey("splitInto")) {
                Integer splitCount = (Integer) table.getExtensions().get("splitInto");
                sql.append(" SPLIT INTO ").append(splitCount).append(" TABLETS");
            }
            // 分片边界
            if (table.getExtensions().containsKey("splitAt")) {
                @SuppressWarnings("unchecked")
                List<String> splitPoints = (List<String>) table.getExtensions().get("splitAt");
                if (splitPoints != null && !splitPoints.isEmpty()) {
                    sql.append(" SPLIT AT VALUES (");
                    sql.append(splitPoints.stream().collect(Collectors.joining(", ")));
                    sql.append(")");
                }
            }
            // 表空间
            if (table.getExtensions().containsKey("tablespace")) {
                sql.append(" TABLESPACE ").append(table.getExtensions().get("tablespace"));
            }
        }
    }

    /**
     * 构建创建表空间语句
     */
    public String buildCreateTablespace(String tablespaceName, String replicaPlacement) {
        return "CREATE TABLESPACE " + quoteIdentifier(null, tablespaceName) + " WITH (replica_placement = '" + replicaPlacement + "')";
    }

    /**
     * 构建设置表复制放置语句
     */
    public String buildSetReplicaPlacement(String tableName, String replicaPlacement) {
        return "ALTER TABLE " + quoteIdentifier(null, tableName) + " SET (replica_placement = '" + replicaPlacement + "')";
    }
}
