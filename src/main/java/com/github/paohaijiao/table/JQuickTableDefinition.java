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
package com.github.paohaijiao.table;

import com.github.paohaijiao.column.JQuickColumnDefinition;
import com.github.paohaijiao.extra.JQuickForeignKeyConstraint;
import com.github.paohaijiao.extra.JQuickIndexDefinition;
import com.github.paohaijiao.extra.JQuickPrimaryKeyConstraint;
import com.github.paohaijiao.extra.JQuickUniqueConstraint;
import lombok.Data;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * packageName com.github.paohaijiao.table
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/13
 */
@Data
public class JQuickTableDefinition {

    private String tableName;

    private boolean quoteEnabled=false;

    private String comment;

    private List<JQuickColumnDefinition> columns = new ArrayList<>();

    private List<JQuickPrimaryKeyConstraint> primaryKeys = new ArrayList<>();

    private List<JQuickUniqueConstraint> uniqueConstraints = new ArrayList<>();

    private List<JQuickIndexDefinition> indexes = new ArrayList<>();

    private List<JQuickForeignKeyConstraint> foreignKeys = new ArrayList<>();

    private Map<String, Object> extensions = new HashMap<>(); // 数据库特定扩展
}
