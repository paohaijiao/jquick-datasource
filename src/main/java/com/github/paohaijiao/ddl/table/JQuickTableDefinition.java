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
package com.github.paohaijiao.ddl.table;

import com.github.paohaijiao.ddl.column.JQuickColumnDefinition;
import com.github.paohaijiao.ddl.dialect.JQuickDatabaseDialect;
import com.github.paohaijiao.ddl.index.JQuickIndexDefinition;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * packageName com.github.paohaijiao.table
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/1/30
 */
@Data
public class JQuickTableDefinition {

    private String tableName;

    private List<JQuickColumnDefinition> columns = new ArrayList<>();

    private List<String> primaryKeys = new ArrayList<>();

    private List<JQuickIndexDefinition> indexes = new ArrayList<>();

    private Map<String, Object> tableOptions = new HashMap<>();

    private JQuickDatabaseDialect dialect = JQuickDatabaseDialect.MYSQL;

    @Override
    public String toString() {
        if (tableName == null || tableName.trim().isEmpty()) {
            return "";
        }
        if (columns == null || columns.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("CREATE TABLE ").append(tableName).append(" (\n");
        List<String> columnDefs = new ArrayList<>();
        for (JQuickColumnDefinition column : columns) {
            columnDefs.add("  " + column.toString());
        }
        if (primaryKeys != null && !primaryKeys.isEmpty()) {
            String pkColumns = primaryKeys.stream().collect(Collectors.joining(", "));
            columnDefs.add("  PRIMARY KEY (" + pkColumns + ")");
        }
        if (indexes != null && !indexes.isEmpty()) {
            for (JQuickIndexDefinition index : indexes) {
                if (index != null) {
                    columnDefs.add("  " + index.toString());
                }
            }
        }
        sb.append(String.join(",\n", columnDefs));
        sb.append("\n)");
        if (tableOptions != null && !tableOptions.isEmpty()) {
            sb.append(" ");
            String options = tableOptions.entrySet().stream()
            .map(entry -> entry.getKey() + "=" + entry.getValue())
             .collect(Collectors.joining(" "));
            sb.append(options);
        }
        switch (dialect) {
            case MYSQL:
            case SQLITE:
                sb.append(";");
                break;
            case ORACLE:
                sb.append(";");
                break;
            case POSTGRESQL:
                sb.append(";");
                break;
            case SQL_SERVER:
                sb.append(";");
                break;
            default:
                sb.append(";");
        }

        return sb.toString();
    }
}
