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
import com.github.paohaijiao.dataType.JQuickDataType;
import com.github.paohaijiao.dataType.JQuickDataTypeConverter;
import com.github.paohaijiao.dataType.impl.JQuickInformixDataTypeConverter;
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
 * Informix 方言实现
 * 支持 IBM Informix 数据库的 SQL 语法特性
 *
 * Informix 版本兼容性：
 * - Informix 12.10+
 * - Informix 14.10+
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/15
 */
public class JQuickInformixDialect extends JQuickAbsSQLDialect {

    protected static final String INFORMIX_QUOTE = "\"";

    private static final String SEQ_SUFFIX = "_SEQ";

    @Override
    protected JQuickDataTypeConverter createDataTypeConvert() {
        return new JQuickInformixDataTypeConverter();
    }

    @Override
    protected String getQuoteKeyWord() {
        return INFORMIX_QUOTE;
    }

    @Override
    public String getAutoIncrementKeyword(JQuickTableDefinition tableDefinition) {
        // Informix 使用 SERIAL 或 BIGSERIAL
        return "SERIAL";
    }

    @Override
    protected void appendTableOptions(StringBuilder sql, JQuickTableDefinition table) {
        if (table.getExtensions() != null) {
            if (table.getExtensions().containsKey("lockMode")) {
                String lockMode = table.getExtensions().get("lockMode").toString();
                sql.append(" LOCK MODE ").append(lockMode.toUpperCase());
            }
            if (table.getExtensions().containsKey("extentSize")) {
                Integer extentSize = (Integer) table.getExtensions().get("extentSize");
                if (extentSize != null) {
                    sql.append(" EXTENT SIZE ").append(extentSize);
                }
            }
            // 下一个扩展数据块大小
            if (table.getExtensions().containsKey("nextExtentSize")) {
                Integer nextExtentSize = (Integer) table.getExtensions().get("nextExtentSize");
                if (nextExtentSize != null) {
                    sql.append(" NEXT SIZE ").append(nextExtentSize);
                }
            }
            // 表空间（dbspace）
            if (table.getExtensions().containsKey("dbspace")) {
                String dbspace = table.getExtensions().get("dbspace").toString();
                sql.append(" IN ").append(quoteIdentifier(table, dbspace));
            }
            // 日志选项
            if (table.getExtensions().containsKey("logging")) {
                Boolean logging = (Boolean) table.getExtensions().get("logging");
                if (logging != null && logging) {
                    sql.append(" WITH LOG");
                } else {
                    sql.append(" WITHOUT LOG");
                }
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
                sql.append("CREATE TEMP TABLE ");
            } else {
                sql.append("CREATE TABLE ");
            }
        } else {
            sql.append("CREATE TABLE ");
        }
        sql.append(quoteIdentifier(table, table.getTableName())).append(" (\n");
        appendColumnDefinitions(sql, table);
        appendPrimaryKey(sql, table);
        appendUniqueConstraints(sql, table);
        appendForeignKeys(sql, table);
        appendCheckConstraints(sql, table);
        removeTrailingComma(sql);
        sql.append(")");
        appendTableOptions(sql, table);
        // 临时表的额外选项
        if (isTemporary && table.getExtensions() != null) {
            if (table.getExtensions().containsKey("withNoLog")) {
                Boolean withNoLog = (Boolean) table.getExtensions().get("withNoLog");
                if (withNoLog != null && withNoLog) {
                    sql.append(" WITH NO LOG");
                }
            }
        }
        // 添加表注释（Informix 使用 COMMENT ON 语句）
        String tableComment = buildTableComment(table);
        if (tableComment != null && !tableComment.isEmpty()) {
            sql.append(";\n").append(tableComment);
        }

        return sql.toString();
    }

    /**
     * 追加检查约束
     */
    protected void appendCheckConstraints(StringBuilder sql, JQuickTableDefinition table) {
        if (table.getExtensions() != null && table.getExtensions().containsKey("checkConstraints")) {
            @SuppressWarnings("unchecked")
            List<Map<String, Object>> checks = (List<Map<String, Object>>) table.getExtensions().get("checkConstraints");
            if (checks != null) {
                for (Map<String, Object> check : checks) {
                    String constraintName = (String) check.get("name");
                    String condition = (String) check.get("condition");
                    if (condition != null && !condition.isEmpty()) {
                        sql.append(INDENT);
                        if (constraintName != null && !constraintName.isEmpty()) {
                            sql.append("CONSTRAINT ").append(quoteIdentifier(table, constraintName)).append(" ");
                        }
                        sql.append("CHECK (").append(condition).append(")");
                        sql.append(COMMA).append(NEW_LINE);
                    }
                }
            }
        }
    }

    /**
     * 构建表注释语句
     */
    protected String buildTableComment(JQuickTableDefinition table) {
        if (table.getComment() != null && !table.getComment().isEmpty()) {
            return "COMMENT ON TABLE " + quoteIdentifier(table, table.getTableName()) + " IS '" + escapeString(table.getComment()) + "'";
        }
        return null;
    }

    /**
     * 构建列注释语句
     */
    public String buildColumnComment(JQuickTableDefinition table, String tableName, JQuickColumnDefinition column) {
        if (column.getComment() != null && !column.getComment().isEmpty()) {
            return "COMMENT ON COLUMN " + quoteIdentifier(table, tableName) + "." + quoteIdentifier(table, column.getColumnName()) + " IS '" + escapeString(column.getComment()) + "'";
        }
        return null;
    }

    /**
     * 构建所有列注释语句
     */
    public String[] buildAllColumnComments(JQuickTableDefinition table, String tableName) {
        return table.getColumns().stream()
                .map(col -> buildColumnComment(table, tableName, col))
                .filter(comment -> comment != null && !comment.isEmpty())
                .toArray(String[]::new);
    }

    /**
     * 构建完整建表语句（包含注释）
     */
    public String buildCreateTableWithComments(JQuickTableDefinition table) {
        StringBuilder fullSql = new StringBuilder();
        fullSql.append(buildCreateTable(table));
        for (JQuickColumnDefinition column : table.getColumns()) {
            String columnComment = buildColumnComment(table, table.getTableName(), column);
            if (columnComment != null && !columnComment.isEmpty()) {
                fullSql.append(";\n").append(columnComment);
            }
        }
        return fullSql.toString();
    }

    @Override
    public String buildColumnDefinition(JQuickTableDefinition table, JQuickColumnDefinition column) {
        StringBuilder def = new StringBuilder();
        def.append(quoteIdentifier(table, column.getColumnName())).append(SPACE);
        // 自增列处理
        if (column.isAutoIncrement()) {
            String serialType = getSerialType(column);
            def.append(serialType);
        } else {
            def.append(getDataTypeString(table, column.getDataType()));
        }
        appendNullClause(def, column);
        appendDefaultClause(def, column);
        return def.toString();
    }

    /**
     * 获取 SERIAL 类型
     */
    private String getSerialType(JQuickColumnDefinition column) {
        JQuickDataType dataType = column.getDataType();
        if (dataType != null) {
            if (dataType.getFamily() != null) {
                switch (dataType.getFamily()) {
                    case LONG:
                        return "BIGSERIAL";
                    case INTEGER:
                        return "SERIAL";
                    default:
                        return "SERIAL";
                }
            }
        }
        return "SERIAL";
    }

    @Override
    protected void appendNullClause(StringBuilder def, JQuickColumnDefinition column) {
        if (!column.isNullable()) {
            def.append(" NOT NULL");
        }
    }

    @Override
    protected void appendDefaultClause(StringBuilder def, JQuickColumnDefinition column) {
        if (column.getDefaultValue() != null && !column.isAutoIncrement()) {
            def.append(" DEFAULT ").append(formatDefaultValue(column.getDefaultValue()));
        }
    }

    @Override
    protected void appendColumnComment(StringBuilder def, JQuickColumnDefinition column) {
        // Informix 使用 COMMENT ON 语句，不在列定义中
    }

    @Override
    protected String getSpecialDefaultKeyword(String value) {
        if (value == null) {
            return null;
        }
        String upperValue = value.toUpperCase();
        if ("CURRENT".equals(upperValue) || "CURRENT_TIMESTAMP".equals(upperValue)) {
            return "CURRENT";
        }
        if ("TODAY".equals(upperValue) || "CURRENT_DATE".equals(upperValue)) {
            return "TODAY";
        }
        if ("CURRENT_TIME".equals(upperValue)) {
            return "CURRENT";
        }
        if ("USER".equals(upperValue)) {
            return "USER";
        }
        if ("CURRENT_USER".equals(upperValue)) {
            return "USER";
        }
        return null;
    }

    @Override
    protected String formatBooleanDefault(boolean value) {
        return value ? "'t'" : "'f'";
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
    public String buildModifyColumn(JQuickTableDefinition table, String tableName, JQuickColumnDefinition column) {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ").append(quoteIdentifier(table, tableName));
        sb.append(" MODIFY ").append(quoteIdentifier(table, column.getColumnName()));
        sb.append(" ").append(getDataTypeString(table, column.getDataType()));
        if (!column.isNullable()) {
            sb.append(" NOT NULL");
        }
        if (column.getDefaultValue() != null && !column.isAutoIncrement()) {
            sb.append(" DEFAULT ").append(formatDefaultValue(column.getDefaultValue()));
        }
        return sb.toString();
    }

    @Override
    public String buildChangeColumn(JQuickTableDefinition table, String tableName, String oldName, JQuickColumnDefinition newColumn) {
        StringBuilder sb = new StringBuilder();
        if (!oldName.equals(newColumn.getColumnName())) {
            sb.append("ALTER TABLE ").append(quoteIdentifier(table, tableName));
            sb.append(" RENAME COLUMN ").append(quoteIdentifier(table, oldName));
            sb.append(" TO ").append(quoteIdentifier(table, newColumn.getColumnName()));
            sb.append(";\n");
        }
        sb.append("ALTER TABLE ").append(quoteIdentifier(table, tableName));
        sb.append(" MODIFY ").append(quoteIdentifier(table, newColumn.getColumnName()));
        sb.append(" ").append(getDataTypeString(table, newColumn.getDataType()));
        if (!newColumn.isNullable()) {
            sb.append(" NOT NULL");
        }
        if (newColumn.getDefaultValue() != null && !newColumn.isAutoIncrement()) {
            sb.append(" DEFAULT ").append(formatDefaultValue(newColumn.getDefaultValue()));
        }
        return sb.toString();
    }

    @Override
    public String buildShowCreateTable(JQuickTableDefinition table, String tableName) {
        return "SELECT 'CREATE TABLE ' || TABNAME || ' (...)' FROM SYSTABLES WHERE TABNAME = '" + tableName.toUpperCase() + "'";
    }

    @Override
    public String buildDescribeTable(JQuickTableDefinition table, String tableName) {
        return "SELECT COLNAME, COLTYPE, COLLENGTH, COLNO " +
                "FROM SYSTABLES t, SYSCOLUMNS c " +
                "WHERE t.TABNAME = '" + tableName.toUpperCase() + "' " +
                "AND t.TABID = c.TABID " +
                "ORDER BY c.COLNO";
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

        return sb.toString();
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
        // Informix 支持 FIRST n 语法
        if (table.getExtensions() != null && table.getExtensions().containsKey("first")) {
            Integer first = (Integer) table.getExtensions().get("first");
            if (first != null && first > 0) {
                sb.append("FIRST ").append(first).append(" ");
            }
        }
        // SKIP n 语法（跳过前n行）
        if (table.getExtensions() != null && table.getExtensions().containsKey("skip")) {
            Integer skip = (Integer) table.getExtensions().get("skip");
            if (skip != null && skip > 0) {
                sb.append("SKIP ").append(skip).append(" ");
            }
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
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE ");
        if (index.isUnique()) {
            sb.append("UNIQUE ");
        }
        // 聚集索引
        if (index.getType() != null && "CLUSTERED".equalsIgnoreCase(index.getType())) {
            sb.append("CLUSTER ");
        }
        // 填充因子
        if (index.hasExtension("fillFactor")) {
            Integer fillFactor = index.getExtension("fillFactor");
            if (fillFactor != null && fillFactor > 0) {
                sb.append("FILLFACTOR ").append(fillFactor).append(" ");
            }
        }
        sb.append("INDEX ").append(quoteIdentifier(table, index.getIndexName()));
        sb.append(" ON ").append("${tableName}").append(" (");
        sb.append(formatColumnList(table, index.getColumns()));
        sb.append(")");
        // 排序方向
        if (index.hasExtension("order")) {
            @SuppressWarnings("unchecked")
            Map<String, String> orderMap = index.getExtension("order");
            if (orderMap != null && !orderMap.isEmpty()) {
                sb.append(" ").append(orderMap.values().stream().findFirst().orElse(""));
            }
        }
        // 表空间
        String dbspace = index.getFileGroup();
        if (dbspace != null && !dbspace.isEmpty()) {
            sb.append(" IN ").append(quoteIdentifier(table, dbspace));
        }
        return sb.toString();
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
                sb.append(" INCREMENT BY ").append(seqParams.get("increment"));
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
        return "SELECT " + sequenceName + ".NEXTVAL FROM SYSTABLES WHERE TABID = 1";
    }

    /**
     * 构建获取序列当前值语句
     */
    public String buildGetSequenceCurrentValue(String sequenceName) {
        return "SELECT " + sequenceName + ".CURRVAL FROM SYSTABLES WHERE TABID = 1";
    }
    /**
     * 构建添加列语句
     */
    @Override
    public String buildAddColumn(JQuickTableDefinition table, String tableName, JQuickColumnDefinition column) {
        StringBuilder sb = new StringBuilder();
        sb.append("ALTER TABLE ").append(quoteIdentifier(table, tableName));
        sb.append(" ADD ").append(quoteIdentifier(table, column.getColumnName()));
        sb.append(" ").append(getDataTypeString(table, column.getDataType()));
        if (!column.isNullable()) {
            sb.append(" NOT NULL");
        }
        if (column.getDefaultValue() != null && !column.isAutoIncrement()) {
            sb.append(" DEFAULT ").append(formatDefaultValue(column.getDefaultValue()));
        }
        // Informix 可以在添加列时指定位置
        if (column.getPosition() != null) {
            if (column.getPosition().getType() != null) {
                switch (column.getPosition().getType()) {
                    case FIRST:
                        sb.append(" BEFORE (FIRST)");
                        break;
                    case AFTER:
                        if (column.getPosition().getAfterColumn() != null) {
                            sb.append(" AFTER ").append(quoteIdentifier(table, column.getPosition().getAfterColumn()));
                        }
                        break;
                }
            }
        }
        return sb.toString();
    }

    /**
     * 构建删除列语句
     */
    @Override
    public String buildDropColumn(JQuickTableDefinition table, String tableName, String columnName) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " DROP " + quoteIdentifier(table, columnName);
    }

    /**
     * 构建重命名列语句
     */
    public String buildRenameColumn(JQuickTableDefinition table, String tableName, String oldName, String newName) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " RENAME COLUMN " + quoteIdentifier(table, oldName) + " TO " + quoteIdentifier(table, newName);
    }

    /**
     * 构建设置 NOT NULL 语句
     */
    public String buildSetNotNull(JQuickTableDefinition table, String tableName, String columnName) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " MODIFY " + quoteIdentifier(table, columnName) + " NOT NULL";
    }

    /**
     * 构建删除 NOT NULL 语句
     */
    public String buildDropNotNull(JQuickTableDefinition table, String tableName, String columnName) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " MODIFY " + quoteIdentifier(table, columnName) + " NULL";
    }

    /**
     * 构建设置默认值语句
     */
    public String buildSetDefaultValue(JQuickTableDefinition table, String tableName, String columnName, Object defaultValue) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " MODIFY " + quoteIdentifier(table, columnName) + " DEFAULT " + formatDefaultValue(defaultValue);
    }

    /**
     * 构建删除默认值语句
     */
    public String buildDropDefaultValue(JQuickTableDefinition table, String tableName, String columnName) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " MODIFY " + quoteIdentifier(table, columnName) + " DEFAULT NULL";
    }
    /**
     * 构建删除表语句
     */
    @Override
    public String buildDropTable(JQuickTableDefinition table, String tableName, boolean ifExists) {
        StringBuilder sb = new StringBuilder();
        if (ifExists) {
            sb.append("BEGIN\n");
            sb.append("  IF (SELECT COUNT(*) FROM SYSTABLES WHERE TABNAME = '").append(tableName.toUpperCase()).append("') > 0 THEN\n");
            sb.append("    DROP TABLE ").append(quoteIdentifier(table, tableName)).append(";\n");
            sb.append("  END IF\n");
            sb.append("END");
        } else {
            sb.append("DROP TABLE ").append(quoteIdentifier(table, tableName));
        }
        return sb.toString();
    }

    /**
     * 构建清空表语句
     */
    @Override
    public String buildTruncateTable(JQuickTableDefinition table, String tableName) {
        // Informix 使用 TRUNCATE 或 DELETE
        return "TRUNCATE TABLE " + quoteIdentifier(table, tableName);
    }

    /**
     * 构建重命名表语句
     */
    @Override
    public String buildRenameTable(JQuickTableDefinition table, String oldName, String newName) {
        return "RENAME TABLE " + quoteIdentifier(table, oldName) + " TO " + quoteIdentifier(table, newName);
    }

    /**
     * 构建锁定表语句
     */
    public String buildLockTable(JQuickTableDefinition table, String tableName, String lockMode) {
        return "LOCK TABLE " + quoteIdentifier(table, tableName) + " IN " + lockMode.toUpperCase() + " MODE";
    }
    /**
     * 构建添加主键语句
     */
    public String buildAddPrimaryKey(JQuickTableDefinition table, String tableName, String constraintName, List<String> columns) {
        StringBuilder sb = new StringBuilder("ALTER TABLE ");
        sb.append(quoteIdentifier(table, tableName));
        sb.append(" ADD CONSTRAINT ");
        if (constraintName != null && !constraintName.isEmpty()) {
            sb.append("PRIMARY KEY (");
        } else {
            sb.append("PRIMARY KEY (");
        }
        sb.append(formatColumnList(table, columns)).append(")");
        if (constraintName != null && !constraintName.isEmpty()) {
            sb.insert(sb.indexOf("PRIMARY KEY"), "CONSTRAINT " + quoteIdentifier(table, constraintName) + " ");
        }
        return sb.toString();
    }

    /**
     * 构建添加唯一约束语句
     */
    public String buildAddUniqueConstraint(JQuickTableDefinition table, String tableName, String constraintName, List<String> columns) {
        StringBuilder sb = new StringBuilder("ALTER TABLE ");
        sb.append(quoteIdentifier(table, tableName));
        sb.append(" ADD CONSTRAINT ");
        if (constraintName != null && !constraintName.isEmpty()) {
            sb.append("CONSTRAINT ").append(quoteIdentifier(table, constraintName)).append(" ");
        }
        sb.append("UNIQUE (").append(formatColumnList(table, columns)).append(")");
        return sb.toString();
    }

    /**
     * 构建添加外键语句
     */
    public String buildAddForeignKey(JQuickTableDefinition table, String tableName, JQuickForeignKeyConstraint fk) {
        StringBuilder sb = new StringBuilder("ALTER TABLE ");
        sb.append(quoteIdentifier(table, tableName));
        sb.append(" ADD CONSTRAINT ");
        if (fk.getConstraintName() != null && !fk.getConstraintName().isEmpty()) {
            sb.append("CONSTRAINT ").append(quoteIdentifier(table, fk.getConstraintName())).append(" ");
        }
        sb.append("FOREIGN KEY (").append(formatColumnList(table, fk.getColumns()));
        sb.append(") REFERENCES ").append(quoteIdentifier(table, fk.getReferencedTable()));
        sb.append(" (").append(formatColumnList(table, fk.getReferencedColumns())).append(")");
        if (fk.getOnDelete() != null) {
            sb.append(" ON DELETE ").append(convertForeignKeyAction(fk.getOnDelete()));
        }
        return sb.toString();
    }

    /**
     * 构建删除约束语句
     */
    public String buildDropConstraint(JQuickTableDefinition table, String tableName, String constraintName) {
        return "ALTER TABLE " + quoteIdentifier(table, tableName) + " DROP CONSTRAINT " + quoteIdentifier(table, constraintName);
    }
    /**
     * 构建创建分片表语句
     */
    public String buildCreateFragmentedTable(JQuickTableDefinition table, String fragmentKey, List<String> fragmentValues) {
        StringBuilder sql = new StringBuilder();
        sql.append(buildCreateTable(table));
        sql.append(" FRAGMENT BY EXPRESSION\n");
        for (int i = 0; i < fragmentValues.size(); i++) {
            String fragmentValue = fragmentValues.get(i);
            sql.append("  ").append(quoteIdentifier(table, fragmentKey)).append(" = ").append(fragmentValue);
            if (i < fragmentValues.size() - 1) {
                sql.append(",\n");
            } else {
                sql.append("\n");
            }
        }
        sql.append("  REMAINDER IN ").append(quoteIdentifier(table, "DBSPACE1"));
        return sql.toString();
    }
    /**
     * 构建分页查询语句（使用 SKIP FIRST 语法）
     */
    public String buildPaginationQuery(JQuickTableDefinition table, List<String> columns, String whereClause, String orderBy, int offset, int limit) {
        StringBuilder sql = new StringBuilder();
        // Informix 使用 SKIP n FIRST n 语法
        if (table.getExtensions() == null) {
            table.setExtensions(new java.util.HashMap<>());
        }
        table.getExtensions().put("skip", offset);
        table.getExtensions().put("first", limit);
        sql.append(buildSelect(table, columns, whereClause));
        if (orderBy != null && !orderBy.isEmpty()) {
            sql.append(" ORDER BY ").append(orderBy);
        }
        return sql.toString();
    }

    /**
     * 构建分页查询语句（使用 ROWNUM 方式，兼容旧版本）
     */
    public String buildLegacyPaginationQuery(JQuickTableDefinition table, List<String> columns, String whereClause, String orderBy, int offset, int limit) {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT * FROM (\n");
        sql.append("  SELECT ROW_NUMBER() OVER (").append(orderBy != null ? "ORDER BY " + orderBy : "").append(") AS ROW_NUM,\n");
        sql.append("    ").append(columns != null && !columns.isEmpty() ?
                columns.stream().map(c -> quoteIdentifier(table, c)).collect(Collectors.joining(", ")) : "*");
        sql.append("\n  FROM ").append(quoteIdentifier(table, table.getTableName()));
        if (whereClause != null && !whereClause.isEmpty()) {
            sql.append("\n  WHERE ").append(whereClause);
        }
        sql.append("\n) WHERE ROW_NUM > ").append(offset);
        if (limit > 0) {
            sql.append(" AND ROW_NUM <= ").append(offset + limit);
        }

        return sql.toString();
    }
    /**
     * 构建更新统计信息语句
     */
    public String buildUpdateStatistics(JQuickTableDefinition table, String tableName) {
        return "UPDATE STATISTICS FOR TABLE " + quoteIdentifier(table, tableName);
    }

    /**
     * 构建更新统计信息语句（高分辨率）
     */
    public String buildUpdateStatisticsHigh(JQuickTableDefinition table, String tableName) {
        return "UPDATE STATISTICS HIGH FOR TABLE " + quoteIdentifier(table, tableName);
    }

    /**
     * 构建更新统计信息语句（中分辨率）
     */
    public String buildUpdateStatisticsMedium(JQuickTableDefinition table, String tableName) {
        return "UPDATE STATISTICS MEDIUM FOR TABLE " + quoteIdentifier(table, tableName);
    }

    /**
     * 构建更新统计信息语句（低分辨率）
     */
    public String buildUpdateStatisticsLow(JQuickTableDefinition table, String tableName) {
        return "UPDATE STATISTICS LOW FOR TABLE " + quoteIdentifier(table, tableName);
    }
    /**
     * 构建查询数据库列表语句
     */
    public String buildShowDatabases() {
        return "SELECT DBSNAME FROM SYSDATABASE";
    }

    /**
     * 构建查询表列表语句
     */
    public String buildShowTables() {
        return "SELECT TABNAME FROM SYSTABLES WHERE TABTYPE = 'T'";
    }

    /**
     * 构建查询当前数据库语句
     */
    public String buildCurrentDatabase() {
        return "SELECT DBINFO('DBNAME') FROM SYSTABLES WHERE TABID = 1";
    }

    /**
     * 构建查询当前用户语句
     */
    public String buildCurrentUser() {
        return "SELECT USER FROM SYSTABLES WHERE TABID = 1";
    }
}
