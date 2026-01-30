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
package com.github.paohaijiao.ddl.index;

import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * packageName com.github.paohaijiao.index
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/1/30
 */
@Data
public class JQuickIndexDefinition {

    private String name;

    private List<String> columns = new ArrayList<>();

    private boolean unique = false;

    private Map<String, Object> indexOptions = new HashMap<>();

    @Override
    public String toString() {
        if (columns == null || columns.isEmpty()) {
            return "";
        }
        StringBuilder sb = new StringBuilder();
        sb.append("PRIMARY KEY (");
        String columnList = columns.stream().collect(Collectors.joining(", "));
        sb.append(columnList);
        sb.append(")");
        if (indexOptions != null && !indexOptions.isEmpty()) {
            sb.append(" ");
            String options = indexOptions.entrySet().stream()
            .map(entry -> entry.getKey() + "=" + entry.getValue())
            .collect(Collectors.joining(" "));
            sb.append(options);
        }
        return sb.toString();
    }
}
