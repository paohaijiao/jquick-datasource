package com.github.paohaijiao.impl;


import com.github.paohaijiao.column.JQuickColumnDefinition;
import com.github.paohaijiao.table.JQuickTableDefinition;

import java.util.List;

/**
 * Redshift 方言实现
 * AWS 云数据仓库，基于 PostgreSQL
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/16
 */
public class JQuickRedshiftDialect extends JQuickPgESDialect {

    @Override
    protected void appendTableOptions(StringBuilder sql, JQuickTableDefinition table) {
        super.appendTableOptions(sql, table);
        if (table.getExtensions() != null) {
            // 分布键
            if (table.getExtensions().containsKey("distkey")) {
                sql.append(" DISTKEY(").append(table.getExtensions().get("distkey")).append(")");
            }

            // 排序键
            if (table.getExtensions().containsKey("sortkey")) {
                String sortType = table.getExtensions().containsKey("sortType") ?
                        table.getExtensions().get("sortType").toString() : "";
                if ("COMPOUND".equalsIgnoreCase(sortType)) {
                    sql.append(" COMPOUND SORTKEY(").append(table.getExtensions().get("sortkey")).append(")");
                } else if ("INTERLEAVED".equalsIgnoreCase(sortType)) {
                    sql.append(" INTERLEAVED SORTKEY(").append(table.getExtensions().get("sortkey")).append(")");
                } else {
                    sql.append(" SORTKEY(").append(table.getExtensions().get("sortkey")).append(")");
                }
            }

            // 备份
            if (table.getExtensions().containsKey("backup")) {
                Boolean backup = (Boolean) table.getExtensions().get("backup");
                sql.append(backup ? " BACKUP YES" : " BACKUP NO");
            }
        }
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
        sql.append(quoteIdentifier(table, table.getTableName())).append(" (\n");
        List<JQuickColumnDefinition> columns = table.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            JQuickColumnDefinition column = columns.get(i);
            sql.append(INDENT).append(quoteIdentifier(table, column.getColumnName()))
                    .append(" ").append(getDataTypeString(table, column.getDataType()));
            if (!column.isNullable()) {
                sql.append(" NOT NULL");
            }
            // Redshift 不支持自增，使用 IDENTITY
            if (column.isAutoIncrement()) {
                sql.append(" IDENTITY(1,1)");
            }
            if (column.getDefaultValue() != null && !column.isAutoIncrement()) {
                sql.append(" DEFAULT ").append(formatDefaultValue(column.getDefaultValue()));
            }
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
     * 构建分析表语句
     */
    public String buildAnalyzeTable(String tableName) {
        return "ANALYZE " + quoteIdentifier(null, tableName);
    }

    /**
     * 构建清空表语句
     */
    @Override
    public String buildTruncateTable(JQuickTableDefinition table, String tableName) {
        return "TRUNCATE TABLE " + quoteIdentifier(table, tableName);
    }

    /**
     * 构建取消查询语句
     */
    public String buildCancelQuery(String processId) {
        return "CANCEL " + processId;
    }

    /**
     * 构建显示查询语句
     */
    public String buildShowQueries() {
        return "SELECT * FROM STV_RECENT";
    }

    /**
     * 构建加载数据语句（从 S3）
     */
    public String buildCopyFromS3(String tableName, String s3Path, String credentials, String region) {
        StringBuilder sb = new StringBuilder();
        sb.append("COPY ").append(quoteIdentifier(null, tableName));
        sb.append(" FROM '").append(s3Path).append("'");
        sb.append("\nCREDENTIALS 'aws_access_key_id=YOUR_KEY;aws_secret_access_key=YOUR_SECRET'");
        if (region != null) {
            sb.append("\nREGION '").append(region).append("'");
        }
        sb.append("\nCSV\nIGNOREHEADER 1");
        return sb.toString();
    }

    /**
     * 构建卸载数据语句（到 S3）
     */
    public String buildUnloadToS3(String tableName, String s3Path) {
        return "UNLOAD ('SELECT * FROM " + quoteIdentifier(null, tableName) + "') TO '" + s3Path + "' CSV HEADER ALLOWOVERWRITE";
    }

    private boolean isTemporary(JQuickTableDefinition table) {
        return table.getExtensions() != null && table.getExtensions().containsKey("temporary") && (Boolean) table.getExtensions().get("temporary");
    }

    @Override
    protected String getSpecialDefaultKeyword(String value) {
        if (value == null) return null;
        String upper = value.toUpperCase();
        if ("GETDATE()".equals(upper) || "NOW()".equals(upper)) return "GETDATE()";
        if ("SYSDATE".equals(upper)) return "SYSDATE";
        return null;
    }
}
