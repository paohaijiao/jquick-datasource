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
package com.github.paohaijiao.abs;
import com.github.paohaijiao.ddl.table.JQuickTableDefinition;
import com.github.paohaijiao.ddl.column.JQuickColumnDefinition;
import com.github.paohaijiao.ddl.index.JQuickIndexDefinition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * packageName com.github.paohaijiao.abs
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/1/31
 */
public abstract class AbstractTableBuilder implements TableBuilder {

    @Override
    public String buildCreateTable(JQuickTableDefinition tableDefinition) {
        StringBuilder sb = new StringBuilder();
        sb.append(buildCreateTableStart(tableDefinition.getTableName()));
        sb.append(buildColumnsSection(tableDefinition.getColumns()));
        sb.append(buildConstraintsSection(tableDefinition));
        sb.append(buildTableEnd());
        sb.append(buildTableOptions(tableDefinition.getTableOptions()));
        sb.append(getStatementTerminator());
        return sb.toString();
    }
    protected abstract String getCreateTableKeyword();

    protected abstract String getNotNullKeyword();

    protected abstract String getPrimaryKeyKeyword();

    protected abstract String getAutoIncrementKeyword();

    protected abstract String getDefaultKeyword();

    protected abstract String getCommentKeyword();

    protected abstract String getUniqueKeyword();

    protected abstract String getIndexKeyword();

    protected abstract String getStatementTerminator();

    protected String escapeIdentifier(String identifier) {
        return identifier;
    }

    protected String formatColumnType(String type, Integer length, Integer precision, Integer scale) {
        StringBuilder typeBuilder = new StringBuilder(type);
        if (length != null && length > 0) {
            typeBuilder.append("(").append(length).append(")");
        } else if (precision != null && scale != null) {
            typeBuilder.append("(").append(precision).append(",").append(scale).append(")");
        } else if (precision != null) {
            typeBuilder.append("(").append(precision).append(")");
        }
        return typeBuilder.toString();
    }

    protected String formatComment(String comment) {
        return comment;
    }

    private String buildCreateTableStart(String tableName) {
        return getCreateTableKeyword() + " " + escapeIdentifier(tableName) + " (\n";
    }

    private String buildColumnsSection(List<JQuickColumnDefinition> columns) {
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < columns.size(); i++) {
            sb.append("  ").append(buildColumnDefinition(columns.get(i)));
            if (i < columns.size() - 1) {
                sb.append(",");
            }
            sb.append("\n");
        }
        return sb.toString();
    }

    private String buildConstraintsSection(JQuickTableDefinition tableDefinition) {
        StringBuilder sb = new StringBuilder();
        List<String> constraints = new ArrayList<>();
        if (tableDefinition.getPrimaryKeys() != null && !tableDefinition.getPrimaryKeys().isEmpty()) {
            constraints.add(buildPrimaryKeyConstraint(tableDefinition.getPrimaryKeys()));
        }
        if (tableDefinition.getIndexes() != null) {
            for (JQuickIndexDefinition index : tableDefinition.getIndexes()) {
                if (index != null) {
                    constraints.add(buildIndexDefinition(index));
                }
            }
        }
        if (!constraints.isEmpty()) {
            for (int i = 0; i < constraints.size(); i++) {
                sb.append("  ").append(constraints.get(i));
                if (i < constraints.size() - 1) {
                    sb.append(",");
                }
                sb.append("\n");
            }
        }

        return sb.toString();
    }

    private String buildTableEnd() {
        return ")";
    }

    @Override
    public String buildColumnDefinition(JQuickColumnDefinition column) {
        StringBuilder sb = new StringBuilder();
        sb.append(escapeIdentifier(column.getName())).append(" ");
        sb.append(formatColumnType(column.getType(), column.getLength(), column.getPrecision(), column.getScale()));
        if (!column.isNullable()) {
            sb.append(" ").append(getNotNullKeyword());
        }
        if (column.getDefaultValue() != null && !column.getDefaultValue().trim().isEmpty()) {
            sb.append(" ").append(getDefaultKeyword()).append(" ").append(column.getDefaultValue());
        }
        if (column.isAutoIncrement()) {
            sb.append(" ").append(getAutoIncrementKeyword());
        }
        if (column.getComment() != null && !column.getComment().trim().isEmpty()) {
            sb.append(" ").append(getCommentKeyword()).append(" ").append(formatComment(column.getComment()));
        }
        if (column.getColumnOptions() != null && !column.getColumnOptions().isEmpty()) {
            sb.append(" ").append(formatOptions(column.getColumnOptions()));
        }
        return sb.toString();
    }

    @Override
    public String buildPrimaryKeyConstraint(List<String> primaryKeys) {
        String columns = primaryKeys.stream().map(this::escapeIdentifier).collect(Collectors.joining(", "));
        return getPrimaryKeyKeyword() + " (" + columns + ")";
    }

    @Override
    public String buildIndexDefinition(JQuickIndexDefinition index) {
        StringBuilder sb = new StringBuilder();
        if (index.isUnique()) {
            sb.append(getUniqueKeyword()).append(" ");
        }
        sb.append(getIndexKeyword()).append(" ");
        sb.append(escapeIdentifier(index.getName())).append(" ");
        String columns = index.getColumns().stream().map(this::escapeIdentifier).collect(Collectors.joining(", "));
        sb.append("(").append(columns).append(")");
        if (index.getIndexOptions() != null && !index.getIndexOptions().isEmpty()) {
            sb.append(" ").append(formatOptions(index.getIndexOptions()));
        }
        return sb.toString();
    }

    @Override
    public String buildTableOptions(Map<String, Object> tableOptions) {
        if (tableOptions == null || tableOptions.isEmpty()) {
            return "";
        }
        return " " + formatOptions(tableOptions);
    }

    protected String formatOptions(Map<String, Object> options) {
        return options.entrySet().stream().map(entry -> entry.getKey() + "=" + entry.getValue()).collect(Collectors.joining(" "));
    }
}
