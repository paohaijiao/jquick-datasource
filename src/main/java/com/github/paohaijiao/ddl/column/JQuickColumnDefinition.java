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
package com.github.paohaijiao.ddl.column;

import lombok.Data;

import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * packageName com.github.paohaijiao.column
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/1/30
 */
@Data
public class JQuickColumnDefinition {

    private String name;

    private String type;

    private Integer length;

    private Integer precision;

    private Integer scale;

    private boolean nullable = true;

    private boolean primaryKey = false;

    private boolean autoIncrement = false;

    private String defaultValue;

    private String comment;

    private Map<String, Object> columnOptions = new HashMap<>();

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append(name);
        sb.append(" ").append(type);
        if (length != null && length > 0) {
            sb.append("(").append(length).append(")");
        } else if (precision != null && scale != null) {
            sb.append("(").append(precision).append(",").append(scale).append(")");
        } else if (precision != null) {
            sb.append("(").append(precision).append(")");
        }
        if (!nullable) {
            sb.append(" NOT NULL");
        }
        if (defaultValue != null && !defaultValue.trim().isEmpty()) {
            sb.append(" DEFAULT ").append(defaultValue);
        }
        if (autoIncrement) {
            sb.append(" AUTO_INCREMENT");
        }
        if (comment != null && !comment.trim().isEmpty()) {
            sb.append(" COMMENT '").append(comment).append("'");
        }
        if (columnOptions != null && !columnOptions.isEmpty()) {
            sb.append(" ");
            String options = columnOptions.entrySet().stream()
            .map(entry -> entry.getKey() + "=" + entry.getValue())
             .collect(Collectors.joining(" "));
            sb.append(options);
        }
        return sb.toString();
    }
}
