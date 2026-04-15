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
package com.github.paohaijiao.dialect;

import com.github.paohaijiao.column.JQuickColumnDefinition;
import com.github.paohaijiao.dataType.JQuickDataType;
import com.github.paohaijiao.dataType.JQuickDataTypeConverter;
import com.github.paohaijiao.dataType.enums.JQuickDataTypeFamily;
import com.github.paohaijiao.enums.JQuickForeignKeyAction;
import com.github.paohaijiao.enums.JQuickPositionType;
import com.github.paohaijiao.extra.JQuickColumnPosition;
import com.github.paohaijiao.extra.JQuickForeignKeyConstraint;
import com.github.paohaijiao.extra.JQuickPrimaryKeyConstraint;
import com.github.paohaijiao.extra.JQuickUniqueConstraint;
import com.github.paohaijiao.table.JQuickTableDefinition;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.stream.Collectors;

/**
 * SQL方言抽象基类
 * 提供公共的流程和方法实现，子类可覆盖特定方法以适应不同数据库
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/13
 */
public abstract class JQuickAbsSQLDialect implements JQuickSQLDialect {
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");

    protected static final String NEW_LINE = "\n";

    protected static final String INDENT = "  ";

    protected static final String COMMA = ",";

    protected static final String SPACE = " ";

    protected abstract JQuickDataTypeConverter createDataTypeConvert();

    protected abstract String getQuoteKeyWord();


    protected abstract void appendTableOptions(StringBuilder sql, JQuickTableDefinition table);

    /**
     * 转换数据类型（子类必须实现）
     *
     * @param family   数据类型家族
     * @param dataType 数据类型对象（包含参数）
     * @return 数据库特定的数据类型字符串
     */
    public String convertDataType(JQuickDataTypeFamily family, JQuickDataType dataType){
        JQuickDataTypeConverter converter=this.createDataTypeConvert();
        return converter.convert(family, dataType);
    }

    @Override
    public String buildCreateTable(JQuickTableDefinition table) {
        StringBuilder sql = new StringBuilder();
        sql.append("CREATE TABLE ").append(quoteIdentifier(table.getTableName())).append(" (\n");
        appendColumnDefinitions(sql, table);
        appendPrimaryKey(sql, table);
        appendUniqueConstraints(sql, table);
        appendForeignKeys(sql, table);
        removeTrailingComma(sql);
        sql.append(")");
        appendTableOptions(sql, table);
        return sql.toString();
    }

    /**
     * 追加列定义
     */
    protected void appendColumnDefinitions(StringBuilder sql, JQuickTableDefinition table) {
        List<JQuickColumnDefinition> columns = table.getColumns();
        for (int i = 0; i < columns.size(); i++) {
            sql.append(INDENT).append(buildColumnDefinition(columns.get(i)));
            boolean hasMore = i < columns.size() - 1 ||
                    !table.getPrimaryKeys().isEmpty() ||
                    !table.getUniqueConstraints().isEmpty() ||
                    !table.getForeignKeys().isEmpty();

            if (hasMore) {
                sql.append(COMMA);
            }
            sql.append(NEW_LINE);
        }
    }

    /**
     * 追加主键
     */
    protected void appendPrimaryKey(StringBuilder sql, JQuickTableDefinition table) {
        if (!table.getPrimaryKeys().isEmpty()) {
            sql.append(INDENT).append(buildPrimaryKey(table.getPrimaryKeys().get(0)));
            if (!table.getUniqueConstraints().isEmpty() || !table.getForeignKeys().isEmpty()) {
                sql.append(COMMA);
            }
            sql.append(NEW_LINE);
        }
    }

    /**
     * 追加唯一约束
     */
    protected void appendUniqueConstraints(StringBuilder sql, JQuickTableDefinition table) {
        List<JQuickUniqueConstraint> uniqueConstraints = table.getUniqueConstraints();
        for (int i = 0; i < uniqueConstraints.size(); i++) {
            sql.append(INDENT).append(buildUniqueConstraint(uniqueConstraints.get(i)));
            if (i < uniqueConstraints.size() - 1 || !table.getForeignKeys().isEmpty()) {
                sql.append(COMMA);
            }
            sql.append(NEW_LINE);
        }
    }

    /**
     * 追加外键
     */
    protected void appendForeignKeys(StringBuilder sql, JQuickTableDefinition table) {
        List<JQuickForeignKeyConstraint> foreignKeys = table.getForeignKeys();
        for (int i = 0; i < foreignKeys.size(); i++) {
            sql.append(INDENT).append(buildForeignKey(foreignKeys.get(i)));
            if (i < foreignKeys.size() - 1) {
                sql.append(COMMA);
            }
            sql.append(NEW_LINE);
        }
    }

    /**
     * 移除最后一个多余的逗号
     */
    protected void removeTrailingComma(StringBuilder sql) {
        String sqlStr = sql.toString();
        if (sqlStr.endsWith(COMMA + NEW_LINE)) {
            sql.setLength(sql.length() - 2);
            sql.append(NEW_LINE);
        }
    }

    @Override
    public String buildColumnDefinition(JQuickColumnDefinition column) {
        StringBuilder def = new StringBuilder();
        def.append(quoteIdentifier(column.getColumnName())).append(SPACE);
        def.append(getDataTypeString(column.getDataType()));
        appendNullClause(def, column);
        if (column.isAutoIncrement()) {
            def.append(SPACE).append(getAutoIncrementKeyword());
        }
        appendDefaultClause(def, column);
        appendColumnComment(def, column);
        appendColumnPosition(def, column);
        return def.toString();
    }

    /**
     * 追加 NULL/NOT NULL 子句
     */
    protected void appendNullClause(StringBuilder def, JQuickColumnDefinition column) {
        if (!column.isNullable()) {
            def.append(" NOT NULL");
        } else {
            def.append(" NULL");
        }
    }

    /**
     * 追加默认值子句
     */
    protected void appendDefaultClause(StringBuilder def, JQuickColumnDefinition column) {
        if (column.getDefaultValue() != null) {
            def.append(" DEFAULT ").append(formatDefaultValue(column.getDefaultValue()));
        }
    }

    /**
     * 追加列注释
     */
    protected void appendColumnComment(StringBuilder def, JQuickColumnDefinition column) {
        if (column.getComment() != null && !column.getComment().isEmpty()) {
            def.append(" COMMENT '").append(escapeString(column.getComment())).append("'");
        }
    }

    /**
     * 追加列位置（FIRST / AFTER）
     */
    protected void appendColumnPosition(StringBuilder def, JQuickColumnDefinition column) {
        if (column.getPosition() != null) {
            JQuickColumnPosition position = column.getPosition();
            if (position.getType() == JQuickPositionType.FIRST) {
                def.append(" FIRST");
            } else if (position.getType() == JQuickPositionType.AFTER && position.getAfterColumn() != null) {
                def.append(" AFTER ").append(quoteIdentifier(position.getAfterColumn()));
            }
        }
    }
    @Override
    public String buildPrimaryKey(JQuickPrimaryKeyConstraint pk) {
        StringBuilder sb = new StringBuilder();
        appendConstraintName(sb, pk.getConstraintName());
        sb.append("PRIMARY KEY (");
        sb.append(formatColumnList(pk.getColumns()));
        sb.append(")");
        return sb.toString();
    }

    @Override
    public String buildUniqueConstraint(JQuickUniqueConstraint uc) {
        StringBuilder sb = new StringBuilder();
        appendConstraintName(sb, uc.getConstraintName());
        sb.append("UNIQUE (");
        sb.append(formatColumnList(uc.getColumns()));
        sb.append(")");
        return sb.toString();
    }

    @Override
    public String buildForeignKey(JQuickForeignKeyConstraint fk) {
        StringBuilder sb = new StringBuilder();
        appendConstraintName(sb, fk.getConstraintName());
        sb.append("FOREIGN KEY (");
        sb.append(formatColumnList(fk.getColumns()));
        sb.append(") REFERENCES ").append(quoteIdentifier(fk.getReferencedTable())).append(" (");
        sb.append(formatColumnList(fk.getReferencedColumns()));
        sb.append(")");
        if (fk.getOnDelete() != null) {
            sb.append(" ON DELETE ").append(convertForeignKeyAction(fk.getOnDelete()));
        }
        if (fk.getOnUpdate() != null) {
            sb.append(" ON UPDATE ").append(convertForeignKeyAction(fk.getOnUpdate()));
        }
        return sb.toString();
    }

    @Override
    public String buildComment(JQuickColumnDefinition column) {
        if (column.getComment() != null && !column.getComment().isEmpty()) {
            return "COMMENT '" + escapeString(column.getComment()) + "'";
        }
        return "";
    }

    /**
     * 追加约束名称
     */
    protected void appendConstraintName(StringBuilder sb, String constraintName) {
        if (constraintName != null && !constraintName.isEmpty()) {
            sb.append("CONSTRAINT ").append(quoteIdentifier(constraintName)).append(SPACE);
        }
    }

    /**
     * 格式化列列表（用逗号分隔并添加引号）
     */
    protected String formatColumnList(List<String> columns) {
        if (columns == null || columns.isEmpty()) {
            return "";
        }
        return columns.stream().map(this::quoteIdentifier).collect(Collectors.joining(", "));
    }

    @Override
    public String getDataTypeString(JQuickDataType dataType) {
        if (dataType.getCustomTypeName() != null && !dataType.getCustomTypeName().isEmpty()) {
            return dataType.getCustomTypeName();
        }
        JQuickDataTypeFamily family = dataType.getFamily();
        if (family == null) {
            return getDefaultDataType();
        }
        return convertDataType(family, dataType);
    }



    /**
     * 获取默认数据类型
     */
    protected String getDefaultDataType() {
        return "VARCHAR(255)";
    }

    /**
     * 标识符引用（MySQL使用反引号，PostgreSQL使用双引号等）
     * 子类可覆盖此方法以使用不同的引用符号
     */
    protected String quoteIdentifier(String identifier) {
        if (identifier == null || identifier.isEmpty()) {
            return identifier;
        }
        if (identifier.startsWith(this.getQuoteKeyWord()) && identifier.endsWith(this.getQuoteKeyWord())) {
            return identifier;
        }
        return  this.getQuoteKeyWord() + identifier + this.getQuoteKeyWord();
    }

    /**
     * 格式化默认值
     */
    protected String formatDefaultValue(Object defaultValue) {
        if (defaultValue == null) {
            return "NULL";
        }
        if (defaultValue instanceof String) {
            String strValue = (String) defaultValue;
            String keyword = getSpecialDefaultKeyword(strValue);
            if (keyword != null) {
                return keyword;
            }
            return "'" + escapeString(strValue) + "'";
        }

        if (defaultValue instanceof Number) {
            return defaultValue.toString();
        }

        if (defaultValue instanceof Boolean) {
            return formatBooleanDefault((Boolean) defaultValue);
        }

        return "'" + escapeString(defaultValue.toString()) + "'";
    }

    /**
     * 获取特殊的默认值关键字（如 CURRENT_TIMESTAMP）
     * 子类可覆盖以支持更多关键字
     */
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
        return null;
    }

    /**
     * 格式化布尔默认值
     */
    protected String formatBooleanDefault(boolean value) {
        return value ? "1" : "0";
    }

    /**
     * 转换外键动作
     */
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

    /**
     * 获取整数类型参数
     */
    protected int getIntParameter(JQuickDataType dataType, String key, int defaultValue) {
        if (dataType.getParameters() != null && dataType.getParameters().containsKey(key)) {
            Object value = dataType.getParameters().get(key);
            if (value instanceof Number) {
                return ((Number) value).intValue();
            }
            if (value instanceof String) {
                try {
                    return Integer.parseInt((String) value);
                } catch (NumberFormatException e) {
                    return defaultValue;
                }
            }
        }
        return defaultValue;
    }

    /**
     * 获取字符串类型参数
     */
    protected String getStringParameter(JQuickDataType dataType, String key, String defaultValue) {
        if (dataType.getParameters() != null && dataType.getParameters().containsKey(key)) {
            Object value = dataType.getParameters().get(key);
            if (value != null) {
                return value.toString();
            }
        }
        return defaultValue;
    }

    /**
     * 转义字符串中的特殊字符
     */
    protected String escapeString(String str) {
        if (str == null) {
            return "";
        }
        return str.replace("\\", "\\\\")
                .replace("'", "\\'")
                .replace("\"", "\\\"")
                .replace("\n", "\\n")
                .replace("\r", "\\r")
                .replace("\t", "\\t");
    }

    /**
     * 构建 ALTER TABLE ADD COLUMN 语句
     */
    public String buildAddColumn(String tableName, JQuickColumnDefinition column) {
        return "ALTER TABLE " + quoteIdentifier(tableName) + " ADD " + buildColumnDefinition(column);
    }

    /**
     * 构建 ALTER TABLE DROP COLUMN 语句
     */
    public String buildDropColumn(String tableName, String columnName) {
        return "ALTER TABLE " + quoteIdentifier(tableName) + " DROP COLUMN " + quoteIdentifier(columnName);
    }

    /**
     * 构建 DROP TABLE 语句
     */
    public String buildDropTable(String tableName, boolean ifExists) {
        StringBuilder sb = new StringBuilder("DROP TABLE ");
        if (ifExists) {
            sb.append("IF EXISTS ");
        }
        sb.append(quoteIdentifier(tableName));
        return sb.toString();
    }

    /**
     * 构建 TRUNCATE TABLE 语句
     */
    public String buildTruncateTable(String tableName) {
        return "TRUNCATE TABLE " + quoteIdentifier(tableName);
    }

    /**
     * 构建 RENAME TABLE 语句
     */
    public String buildRenameTable(String oldName, String newName) {
        return "RENAME TABLE " + quoteIdentifier(oldName) + " TO " + quoteIdentifier(newName);
    }

    public String formatValue(Object value) {
        if (value == null) {
            return "NULL";
        }
        if (value instanceof String) {
            return "'" + escapeString((String) value) + "'";
        }
        if (value instanceof Number || value instanceof Boolean) {
            return value.toString();
        }
        if (value instanceof java.util.Date) {
            String date=sdf.format(value);
            return "'" + date + "'";
        }
        return "'" + escapeString(value.toString()) + "'";
    }
}
