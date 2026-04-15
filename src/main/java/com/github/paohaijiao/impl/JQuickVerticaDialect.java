package com.github.paohaijiao.impl;

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

import com.github.paohaijiao.column.JQuickColumnDefinition;
import com.github.paohaijiao.dataType.JQuickDataTypeConverter;
import com.github.paohaijiao.dataType.impl.JQuickVerticaDataTypeConverter;
import com.github.paohaijiao.dialect.JQuickAbsSQLDialect;
import com.github.paohaijiao.enums.JQuickForeignKeyAction;
import com.github.paohaijiao.extra.JQuickForeignKeyConstraint;
import com.github.paohaijiao.extra.JQuickIndexDefinition;
import com.github.paohaijiao.row.JQuickRow;
import com.github.paohaijiao.table.JQuickTableDefinition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Vertica 方言实现
 * 支持 Vertica 列式存储数据库的 SQL 语法特性
 * <p>
 * Vertica 版本兼容性：
 * - Vertica 9.x
 * - Vertica 10.x
 * - Vertica 11.x
 * - Vertica 12.x
 * <p>
 * Vertica 与传统数据库的差异：
 * - 列式存储，MPP 架构
 * - 支持投影（Projection）而非传统索引
 * - 支持分区和分段（Segmentation）
 * - 支持弹性集群
 * - 支持自动数据压缩
 * - 适合大规模数据仓库和实时分析
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/15
 */
public class JQuickVerticaDialect extends JQuickAbsSQLDialect {

    protected static final String VERTICA_QUOTE = "\"";

    private static final String SEQ_SUFFIX = "_seq";

    @Override
    protected JQuickDataTypeConverter createDataTypeConvert() {
        return new JQuickVerticaDataTypeConverter();
    }

    @Override
    protected String getQuoteKeyWord() {
        return VERTICA_QUOTE;
    }

    @Override
    public String getAutoIncrementKeyword(JQuickTableDefinition tableDefinition) {
        // Vertica 使用 IDENTITY 或 AUTO_INCREMENT
        return "IDENTITY";
    }

    @Override
    protected void appendTableOptions(StringBuilder sql, JQuickTableDefinition table) {
        // Vertica 表选项 - 核心特性
        if (table.getExtensions() != null) {

            // 分段策略（Vertica 核心特性）
            if (table.getExtensions().containsKey("segmentedBy")) {
                String segmentedBy = table.getExtensions().get("segmentedBy").toString();
                sql.append("\nSEGMENTED BY ").append(segmentedBy);
                // 分段数
                if (table.getExtensions().containsKey("segments")) {
                    Integer segments = (Integer) table.getExtensions().get("segments");
                    if (segments != null && segments > 0) {
                        sql.append(" SEGMENTS ").append(segments);
                    }
                } else if (table.getExtensions().containsKey("allSegments")) {
                    Boolean allSegments = (Boolean) table.getExtensions().get("allSegments");
                    if (allSegments != null && allSegments) {
                        sql.append(" ALL SEGMENTS");
                    }
                }
            } else if (table.getExtensions().containsKey("segmentedByAuto")) {
                Boolean auto = (Boolean) table.getExtensions().get("segmentedByAuto");
                if (auto != null && auto) {
                    sql.append("\nSEGMENTED BY AUTO");
                }
            }
            // 组织类型
            if (table.getExtensions().containsKey("organizedBy")) {
                String organizedBy = table.getExtensions().get("organizedBy").toString();
                sql.append("\nORGANIZED BY ").append(organizedBy);
            }
            // 分区
            if (table.getExtensions().containsKey("partitionBy")) {
                String partitionBy = table.getExtensions().get("partitionBy").toString();
                sql.append("\nPARTITION BY ").append(partitionBy);

                // 分区级别
                if (table.getExtensions().containsKey("partitionLevel")) {
                    String level = table.getExtensions().get("partitionLevel").toString();
                    sql.append(" ").append(level.toUpperCase());
                }

                // 分区组
                if (table.getExtensions().containsKey("groupBy")) {
                    String groupBy = table.getExtensions().get("groupBy").toString();
                    sql.append("\nGROUP BY ").append(groupBy);
                }
            }

            // 有序分区
            if (table.getExtensions().containsKey("orderBy")) {
                String orderBy = table.getExtensions().get("orderBy").toString();
                sql.append("\nORDER BY ").append(orderBy);
            }

            // K-Safety（数据冗余）
            if (table.getExtensions().containsKey("ksafety")) {
                Integer ksafety = (Integer) table.getExtensions().get("ksafety");
                if (ksafety != null && ksafety > 0) {
                    sql.append("\nKSAFETY ").append(ksafety);
                }
            }

            // 表空间
            if (table.getExtensions().containsKey("tablespace")) {
                String tablespace = table.getExtensions().get("tablespace").toString();
                sql.append("\nTABLESPACE ").append(quoteIdentifier(table, tablespace));
            }

            // 存储选项
            if (table.getExtensions().containsKey("storage")) {
                @SuppressWarnings("unchecked")
                Map<String, String> storage = (Map<String, String>) table.getExtensions().get("storage");
                if (storage != null && !storage.isEmpty()) {
                    sql.append("\nWITH (");
                    boolean first = true;
                    for (Map.Entry<String, String> entry : storage.entrySet()) {
                        if (!first) {
                            sql.append(", ");
                        }
                        sql.append(entry.getKey()).append(" = ").append(entry.getValue());
                        first = false;
                    }
                    sql.append(")");
                }
            }

            // 压缩选项
            if (table.getExtensions().containsKey("compression")) {
                String compression = table.getExtensions().get("compression").toString();
                sql.append("\nCOMPRESSION ").append(compression.toUpperCase());
            }

            // 表注释
            if (table.getComment() != null && !table.getComment().isEmpty()) {
                sql.append(";\nCOMMENT ON TABLE ").append(quoteIdentifier(table, table.getTableName()))
                        .append(" IS '").append(escapeString(table.getComment())).append("'");
            }
        }
    }

    @Override
    public String buildCreateTable(JQuickTableDefinition table) {
        StringBuilder sql = new StringBuilder();
        // 临时表
        boolean isTemporary = false;
        if (table.getExtensions() != null && table.getExtensions().containsKey("temporary")) {
            Boolean temporary = (Boolean) table.getExtensions().get("temporary");
            if (temporary != null && temporary) {
                isTemporary = true;
                sql.append("CREATE LOCAL TEMPORARY TABLE ");
            } else {
                sql.append("CREATE TABLE ");
            }
        } else {
            sql.append("CREATE TABLE ");
        }

        // IF NOT EXISTS
        if (table.getExtensions() != null && table.getExtensions().containsKey("ifNotExists")) {
            Boolean ifNotExists = (Boolean) table.getExtensions().get("ifNotExists");
            if (ifNotExists != null && ifNotExists) {
                sql.append("IF NOT EXISTS ");
            }
        }
        sql.append(quoteIdentifier(table, table.getTableName()));
        // 列定义
        sql.append(" (\n");
        List<JQuickColumnDefinition> columns = table.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            JQuickColumnDefinition column = columns.get(i);
            sql.append(INDENT).append(quoteIdentifier(table, column.getColumnName()))
                    .append(" ").append(getDataTypeString(table, column.getDataType()));
            // 自增列
            if (column.isAutoIncrement()) {
                sql.append(" ").append(getAutoIncrementKeyword(table));
            }

            // 默认值
            if (column.getDefaultValue() != null && !column.isAutoIncrement()) {
                sql.append(" DEFAULT ").append(formatDefaultValue(column.getDefaultValue()));
            }

            // 列注释
            if (column.getComment() != null && !column.getComment().isEmpty()) {
                sql.append(" COMMENT '").append(escapeString(column.getComment())).append("'");
            }

            if (i < columns.size() - 1) {
                sql.append(",");
            }
            sql.append(NEW_LINE);
        }

        // 约束定义
        if (!table.getPrimaryKeys().isEmpty() || !table.getUniqueConstraints().isEmpty()) {
            if (!table.getColumns().isEmpty()) {
                // 已有列定义，不需要额外逗号
            }

            for (com.github.paohaijiao.extra.JQuickPrimaryKeyConstraint pk : table.getPrimaryKeys()) {
                sql.append(COMMA).append(NEW_LINE);
                sql.append(INDENT).append(buildPrimaryKey(table, pk));
            }

            for (com.github.paohaijiao.extra.JQuickUniqueConstraint uc : table.getUniqueConstraints()) {
                sql.append(COMMA).append(NEW_LINE);
                sql.append(INDENT).append(buildUniqueConstraint(table, uc));
            }
        }

        sql.append("\n)");
        appendTableOptions(sql, table);
        return sql.toString();
    }

    /**
     * 构建创建投影（Projection）语句
     * 投影是 Vertica 的核心特性，类似物化视图
     */
    public String buildCreateProjection(String projectionName, String baseTableName, String columns, String segmentedBy, String orderBy, boolean isSuperProjection) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE PROJECTION ").append(projectionName);
        if (isSuperProjection) {
            sb.append("\nAS SELECT ").append(columns);
            sb.append("\nFROM ").append(baseTableName);
            sb.append("\nSEGMENTED BY ").append(segmentedBy);
            sb.append(" ALL NODES");
        } else {
            sb.append("\n(").append(columns).append(")");
            sb.append("\nAS SELECT ").append(columns);
            sb.append("\nFROM ").append(baseTableName);
            sb.append("\nORDER BY ").append(orderBy);
            sb.append("\nSEGMENTED BY ").append(segmentedBy);
        }

        return sb.toString();
    }

    /**
     * 构建激活投影语句
     */
    public String buildActivateProjection(String projectionName) {
        return "SELECT REFRESH('" + projectionName + "')";
    }

    @Override
    public String buildColumnDefinition(JQuickTableDefinition table, JQuickColumnDefinition column) {
        StringBuilder def = new StringBuilder();
        def.append(quoteIdentifier(table, column.getColumnName())).append(SPACE);
        def.append(getDataTypeString(table, column.getDataType()));
        // 编码类型（Vertica 特有）
        if (column.getExtensions() != null && column.getExtensions().containsKey("encoding")) {
            String encoding = column.getExtensions().get("encoding").toString();
            def.append(" ENCODING ").append(encoding.toUpperCase());
        }

        return def.toString();
    }

    @Override
    protected void appendNullClause(StringBuilder def, JQuickColumnDefinition column) {
        if (!column.isNullable()) {
            def.append(" NOT NULL");
        }
    }

    @Override
    protected void appendDefaultClause(StringBuilder def, JQuickColumnDefinition column) {
        // 已在 buildCreateTable 中处理
    }

    @Override
    protected void appendColumnComment(StringBuilder def, JQuickColumnDefinition column) {
        // 已在 buildCreateTable 中处理
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
        if ("SYSDATE".equals(upperValue)) {
            return "SYSDATE";
        }
        return null;
    }

    @Override
    protected String formatBooleanDefault(boolean value) {
        return value ? "TRUE" : "FALSE";
    }

    @Override
    protected String convertForeignKeyAction(JQuickForeignKeyAction action) {
        if (action == null) {
            return "NO ACTION";
        }
        switch (action) {
            case NO_ACTION:
                return "NO ACTION";
            case RESTRICT:
                return "RESTRICT";
            case CASCADE:
                return "CASCADE";
            case SET_NULL:
                return "SET NULL";
            case SET_DEFAULT:
                return "SET DEFAULT";
            default:
                return "NO ACTION";
        }
    }

    @Override
    public String buildPrimaryKey(JQuickTableDefinition table,
                                  com.github.paohaijiao.extra.JQuickPrimaryKeyConstraint pk) {
        StringBuilder sb = new StringBuilder();
        if (pk.getConstraintName() != null && !pk.getConstraintName().isEmpty()) {
            sb.append("CONSTRAINT ").append(quoteIdentifier(table, pk.getConstraintName())).append(" ");
        }
        sb.append("PRIMARY KEY (");
        sb.append(formatColumnList(table, pk.getColumns()));
        sb.append(")");
        return sb.toString();
    }

    @Override
    public String buildUniqueConstraint(JQuickTableDefinition table,
                                        com.github.paohaijiao.extra.JQuickUniqueConstraint uc) {
        StringBuilder sb = new StringBuilder();
        if (uc.getConstraintName() != null && !uc.getConstraintName().isEmpty()) {
            sb.append("CONSTRAINT ").append(quoteIdentifier(table, uc.getConstraintName())).append(" ");
        }
        sb.append("UNIQUE (");
        sb.append(formatColumnList(table, uc.getColumns()));
        sb.append(")");
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
        if (fk.getOnDelete() != null) {
            sb.append(" ON DELETE ").append(convertForeignKeyAction(fk.getOnDelete()));
        }
        if (fk.getOnUpdate() != null) {
            sb.append(" ON UPDATE ").append(convertForeignKeyAction(fk.getOnUpdate()));
        }
        // Vertica 外键可以设置为 ENABLED/DISABLED
        if (fk.getExtensions() != null && fk.getExtensions().containsKey("enabled")) {
            Boolean enabled = (Boolean) fk.getExtensions().get("enabled");
            if (enabled != null && !enabled) {
                sb.append(" DISABLED");
            }
        }

        return sb.toString();
    }

    @Override
    public String buildModifyColumn(JQuickTableDefinition table, String tableName, JQuickColumnDefinition column) {
        // Vertica 支持 ALTER TABLE ALTER COLUMN
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ").append(quoteIdentifier(table, tableName));
        sb.append(" ALTER COLUMN ").append(quoteIdentifier(table, column.getColumnName()));
        sb.append(" SET DATA TYPE ").append(getDataTypeString(table, column.getDataType()));
        if (!column.isNullable()) {
            sb.append(",\nALTER TABLE ").append(quoteIdentifier(table, tableName));
            sb.append(" ALTER COLUMN ").append(quoteIdentifier(table, column.getColumnName()));
            sb.append(" SET NOT NULL");
        }
        if (column.getDefaultValue() != null && !column.isAutoIncrement()) {
            sb.append(",\nALTER TABLE ").append(quoteIdentifier(table, tableName));
            sb.append(" ALTER COLUMN ").append(quoteIdentifier(table, column.getColumnName()));
            sb.append(" SET DEFAULT ").append(formatDefaultValue(column.getDefaultValue()));
        }
        return sb.toString();
    }

    @Override
    public String buildChangeColumn(JQuickTableDefinition table, String tableName,
                                    String oldName, JQuickColumnDefinition newColumn) {
        StringBuilder sb = new StringBuilder();
        // 重命名列
        if (!oldName.equals(newColumn.getColumnName())) {
            sb.append("ALTER TABLE ").append(quoteIdentifier(table, tableName));
            sb.append(" RENAME COLUMN ").append(quoteIdentifier(table, oldName));
            sb.append(" TO ").append(quoteIdentifier(table, newColumn.getColumnName()));
            sb.append(";\n");
        }

        // 修改类型
        sb.append("ALTER TABLE ").append(quoteIdentifier(table, tableName));
        sb.append(" ALTER COLUMN ").append(quoteIdentifier(table, newColumn.getColumnName()));
        sb.append(" SET DATA TYPE ").append(getDataTypeString(table, newColumn.getDataType()));
        return sb.toString();
    }

    @Override
    public String buildAddColumn(JQuickTableDefinition table, String tableName, JQuickColumnDefinition column) {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ").append(quoteIdentifier(table, tableName));
        sb.append(" ADD COLUMN ").append(quoteIdentifier(table, column.getColumnName()));
        sb.append(" ").append(getDataTypeString(table, column.getDataType()));
        if (!column.isNullable()) {
            sb.append(" NOT NULL");
        }
        if (column.getDefaultValue() != null && !column.isAutoIncrement()) {
            sb.append(" DEFAULT ").append(formatDefaultValue(column.getDefaultValue()));
        }
        if (column.getComment() != null && !column.getComment().isEmpty()) {
            sb.append(";\nCOMMENT ON COLUMN ").append(quoteIdentifier(table, tableName))
                    .append(".").append(quoteIdentifier(table, column.getColumnName()))
                    .append(" IS '").append(escapeString(column.getComment())).append("'");
        }
        return sb.toString();
    }

    @Override
    public String buildDropColumn(JQuickTableDefinition table, String tableName, String columnName) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " DROP COLUMN " + quoteIdentifier(table, columnName) + " CASCADE";
    }

    @Override
    public String buildShowCreateTable(JQuickTableDefinition table, String tableName) {
        return "SELECT EXPORT_TABLES('', '" + tableName + "')";
    }

    @Override
    public String buildDescribeTable(JQuickTableDefinition table, String tableName) {
        return "SELECT " +
                "  column_name, " +
                "  data_type, " +
                "  is_nullable, " +
                "  column_default " +
                "FROM columns " +
                "WHERE table_name = '" + tableName.toUpperCase() + "' " +
                "ORDER BY ordinal_position";
    }

    @Override
    public String buildInsert(JQuickTableDefinition table, JQuickRow row) {
        if (row == null || row.isEmpty() || table == null) {
            return "";
        }
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
        // 返回自增主键
        if (!table.getPrimaryKeys().isEmpty() && hasAutoIncrement(table)) {
            String pkColumn = table.getPrimaryKeys().get(0).getColumns().get(0);
            sb.append(" RETURNING ").append(quoteIdentifier(table, pkColumn));
        }

        return sb.toString();
    }

    /**
     * 构建批量插入语句
     */
    public String buildInsertBatch(JQuickTableDefinition table, List<JQuickRow> rows) {
        if (rows == null || rows.isEmpty() || table == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ").append(quoteIdentifier(table, table.getTableName()));
        List<String> columns = new ArrayList<>(rows.get(0).keySet());
        sb.append(" (").append(columns.stream()
                        .map(e -> quoteIdentifier(table, e))
                        .collect(Collectors.joining(", ")))
                .append(") VALUES ");
        for (int i = 0; i < rows.size(); i++) {
            JQuickRow row = rows.get(i);
            if (i > 0) {
                sb.append(", ");
            }
            sb.append("(");
            List<String> values = new ArrayList<>();
            for (String col : columns) {
                values.add(formatValue(row.get(col)));
            }
            sb.append(String.join(", ", values));
            sb.append(")");
        }

        return sb.toString();
    }

    /**
     * 构建从查询插入的语句
     */
    public String buildInsertFromQuery(JQuickTableDefinition table, String query) {
        return "INSERT INTO " + quoteIdentifier(table, table.getTableName()) + "\n" + query;
    }

    @Override
    public String buildUpdate(JQuickTableDefinition table, JQuickRow row, String whereClause) {
        if (row == null || row.isEmpty() || table == null) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("UPDATE ").append(quoteIdentifier(table, table.getTableName())).append(" SET ");
        List<String> setClauses = new ArrayList<>();
        for (Map.Entry<String, Object> entry : row.entrySet()) {
            setClauses.add(quoteIdentifier(table, entry.getKey()) + " = " + formatValue(entry.getValue()));
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
        sb.append("DELETE FROM ").append(quoteIdentifier(table, table.getTableName()));
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
        // 查询提示
        if (table.getExtensions() != null && table.getExtensions().containsKey("hint")) {
            String hint = table.getExtensions().get("hint").toString();
            sb.append("/*+ ").append(hint).append(" */ ");
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

    @Override
    public String buildIndex(JQuickTableDefinition table, JQuickIndexDefinition index) {
        // Vertica 使用投影而非传统索引，但支持一些索引类型
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE ");
        if (index.isUnique()) {
            sb.append("UNIQUE ");
        }
        sb.append("INDEX ").append(quoteIdentifier(table, index.getIndexName()));
        sb.append(" ON ").append("${tableName}").append(" (");
        sb.append(formatColumnList(table, index.getColumns()));
        sb.append(")");
        // 索引类型
        if (index.getType() != null) {
            sb.append(" USING ").append(index.getType().toUpperCase());
        }

        return sb.toString();
    }

    /**
     * 检查是否有自增列
     */
    private boolean hasAutoIncrement(JQuickTableDefinition table) {
        return table.getColumns().stream().anyMatch(JQuickColumnDefinition::isAutoIncrement);
    }
    /**
     * 构建创建序列语句
     */
    public String buildCreateSequence(JQuickTableDefinition table, String sequenceName) {
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE SEQUENCE ").append(quoteIdentifier(table, sequenceName));
        Map<String, Object> seqParams = null;
        if (table.getExtensions() != null && table.getExtensions().containsKey("sequenceParams")) {
            @SuppressWarnings("unchecked")
            Map<String, Object> params = (Map<String, Object>) table.getExtensions().get("sequenceParams");
            seqParams = params;
        }

        if (seqParams != null) {
            if (seqParams.containsKey("start")) {
                sb.append(" START ").append(seqParams.get("start"));
            }
            if (seqParams.containsKey("increment")) {
                sb.append(" INCREMENT ").append(seqParams.get("increment"));
            }
            if (seqParams.containsKey("minValue")) {
                sb.append(" MINVALUE ").append(seqParams.get("minValue"));
            }
            if (seqParams.containsKey("maxValue")) {
                sb.append(" MAXVALUE ").append(seqParams.get("maxValue"));
            }
            if (seqParams.containsKey("cache")) {
                sb.append(" CACHE ").append(seqParams.get("cache"));
            }
            if (seqParams.containsKey("cycle")) {
                Boolean cycle = (Boolean) seqParams.get("cycle");
                if (cycle != null && cycle) {
                    sb.append(" CYCLE");
                }
            }
        }

        return sb.toString();
    }

    /**
     * 构建删除序列语句
     */
    public String buildDropSequence(JQuickTableDefinition table, String sequenceName) {
        return "DROP SEQUENCE " + quoteIdentifier(table, sequenceName);
    }

    /**
     * 构建获取序列下一个值语句
     */
    public String buildGetSequenceNextValue(String sequenceName) {
        return "SELECT NEXTVAL('" + sequenceName + "')";
    }

    /**
     * 构建获取序列当前值语句
     */
    public String buildGetSequenceCurrentValue(String sequenceName) {
        return "SELECT CURRVAL('" + sequenceName + "')";
    }


    /**
     * 构建添加分区语句
     */
    public String buildAddPartition(JQuickTableDefinition table, String tableName, String partitionValue) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " ADD PARTITION '" + partitionValue + "'";
    }

    /**
     * 构建删除分区语句
     */
    public String buildDropPartition(JQuickTableDefinition table, String tableName, String partitionValue) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) +" DROP PARTITION '" + partitionValue + "'";
    }

    /**
     * 构建移动分区语句
     */
    public String buildMovePartition(JQuickTableDefinition table, String tableName, String partitionValue, String destTableName) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " MOVE PARTITION '" + partitionValue + "' TO TABLE " + quoteIdentifier(table, destTableName);
    }

    /**
     * 构建交换分区语句
     */
    public String buildExchangePartition(JQuickTableDefinition table, String tableName,
                                         String partitionValue, String newTableName) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " EXCHANGE PARTITION '" + partitionValue + "' WITH TABLE " + quoteIdentifier(table, newTableName);
    }

    /**
     * 构建合并分区语句
     */
    public String buildMergePartitions(JQuickTableDefinition table, String tableName,
                                       String partitionFrom, String partitionTo) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " MERGE PARTITIONS '" + partitionFrom + "' TO '" + partitionTo + "'";
    }

    /**
     * 构建显示分区语句
     */
    public String buildShowPartitions(JQuickTableDefinition table, String tableName) {
        return "SELECT partition_key, partition_value, projection_name " + "FROM partitions WHERE table_name = '" + tableName.toUpperCase() + "'";
    }

    /**
     * 构建刷新投影语句
     */
    public String buildRefreshProjection(String projectionName) {
        return "SELECT REFRESH('" + projectionName + "')";
    }

    /**
     * 构建刷新所有投影语句
     */
    public String buildRefreshAllProjections() {
        return "SELECT REFRESH('')";
    }

    /**
     * 构建启动投影刷新语句
     */
    public String buildStartRefreshProjection(String projectionName) {
        return "SELECT START_REFRESH('" + projectionName + "')";
    }

    /**
     * 构建停止投影刷新语句
     */
    public String buildStopRefreshProjection(String projectionName) {
        return "SELECT STOP_REFRESH('" + projectionName + "')";
    }

    /**
     * 构建显示投影语句
     */
    public String buildShowProjections(String tableName) {
        return "SELECT projection_name, projection_type, is_super_projection " + "FROM projections WHERE anchor_table_name = '" + tableName.toUpperCase() + "'";
    }


    /**
     * 构建删除表语句
     */
    @Override
    public String buildDropTable(JQuickTableDefinition table, String tableName, boolean ifExists) {
        StringBuilder sb = new StringBuilder("DROP TABLE ");
        if (ifExists) {
            sb.append("IF EXISTS ");
        }
        sb.append(quoteIdentifier(table, tableName));
        // 级联删除
        if (table.getExtensions() != null && table.getExtensions().containsKey("cascade")) {
            Boolean cascade = (Boolean) table.getExtensions().get("cascade");
            if (cascade != null && cascade) {
                sb.append(" CASCADE");
            }
        }
        return sb.toString();
    }

    /**
     * 构建清空表语句
     */
    @Override
    public String buildTruncateTable(JQuickTableDefinition table, String tableName) {
        return "TRUNCATE TABLE " + quoteIdentifier(table, tableName);
    }

    /**
     * 构建重命名表语句
     */
    @Override
    public String buildRenameTable(JQuickTableDefinition table, String oldName, String newName) {
        return "ALTER TABLE " + quoteIdentifier(table, oldName) + " RENAME TO " + quoteIdentifier(table, newName);
    }


    /**
     * 构建收集统计信息语句
     */
    public String buildCollectStatistics(JQuickTableDefinition table, String tableName) {
        return "SELECT ANALYZE_STATISTICS('" + tableName + "')";
    }

    /**
     * 构建收集列统计信息语句
     */
    public String buildCollectColumnStatistics(String tableName, String columnName) {
        return "SELECT ANALYZE_STATISTICS('" + tableName + "', '" + columnName + "')";
    }

    /**
     * 构建获取统计信息语句
     */
    public String buildGetStatistics(String tableName) {
        return "SELECT * FROM statistics WHERE table_name = '" + tableName.toUpperCase() + "'";
    }

    /**
     * 构建查询计划语句
     */
    public String buildExplain(String query) {
        return "EXPLAIN " + query;
    }

    /**
     * 构建详细查询计划语句
     */
    public String buildExplainExtended(String query) {
        return "EXPLAIN VERBOSE " + query;
    }

    /**
     * 构建查询执行计划语句
     */
    public String buildExplainExecution(String query) {
        return "EXPLAIN EXECUTION " + query;
    }

    /**
     * 构建分析查询语句
     */
    public String buildProfile() {
        return "SELECT * FROM query_profiles";
    }

    /**
     * 构建分页查询语句
     */
    public String buildPaginationQuery(JQuickTableDefinition table, List<String> columns, String whereClause, String orderBy, int offset, int limit) {
        StringBuilder sql = new StringBuilder();
        sql.append(buildSelect(table, columns, whereClause));
        if (orderBy != null && !orderBy.isEmpty()) {
            sql.append(" ORDER BY ").append(orderBy);
        }
        sql.append(" LIMIT ").append(limit);
        if (offset > 0) {
            sql.append(" OFFSET ").append(offset);
        }
        return sql.toString();
    }


    /**
     * 构建创建模式语句
     */
    public String buildCreateSchema(String schemaName) {
        return "CREATE SCHEMA IF NOT EXISTS " + schemaName;
    }

    /**
     * 构建删除模式语句
     */
    public String buildDropSchema(String schemaName, boolean ifExists) {
        StringBuilder sb = new StringBuilder("DROP SCHEMA ");
        if (ifExists) {
            sb.append("IF EXISTS ");
        }
        sb.append(schemaName);
        return sb.toString();
    }

    /**
     * 构建显示模式语句
     */
    public String buildShowSchemas() {
        return "SELECT schema_name FROM schemata";
    }

    /**
     * 构建显示表语句
     */
    public String buildShowTables() {
        return "SELECT table_name FROM tables WHERE table_schema = 'public'";
    }


    /**
     * 构建添加节点语句
     */
    public String buildAddNode(String nodeName, String address) {
        return "SELECT ADD_NODE('" + nodeName + "', '" + address + "')";
    }

    /**
     * 构建删除节点语句
     */
    public String buildRemoveNode(String nodeName) {
        return "SELECT REMOVE_NODE('" + nodeName + "')";
    }

    /**
     * 构建重新平衡语句
     */
    public String buildRebalance() {
        return "SELECT REBALANCE_CLUSTER()";
    }


    /**
     * 构建查询资源池语句
     */
    public String buildShowResourcePools() {
        return "SELECT * FROM resource_pools";
    }

    /**
     * 构建查询会话语句
     */
    public String buildShowSessions() {
        return "SELECT * FROM sessions";
    }

    /**
     * 构建终止会话语句
     */
    public String buildTerminateSession(String sessionId) {
        return "SELECT CLOSE_SESSION('" + sessionId + "')";
    }

    /**
     * 构建查询负载信息语句
     */
    public String buildShowResourceQueue() {
        return "SELECT * FROM resource_queue";
    }


    /**
     * 构建导出数据语句
     */
    public String buildExportToFile(String tableName, String filePath, String format) {
        return "COPY " + tableName + " TO '" + filePath + "' WITH (FORMAT = " + format.toUpperCase() + ")";
    }

    /**
     * 构建导出数据语句（带分隔符）
     */
    public String buildExportToFileDelimited(String tableName, String filePath, String delimiter) {
        return "COPY " + tableName + " TO '" + filePath + "' WITH (FORMAT = 'CSV', DELIMITER = '" + delimiter + "')";
    }
}