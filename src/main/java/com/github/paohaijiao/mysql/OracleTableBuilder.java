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
package com.github.paohaijiao.mysql;

import com.github.paohaijiao.abs.AbstractTableBuilder;
import com.github.paohaijiao.ddl.index.JQuickIndexDefinition;

import java.util.Map;

/**
 * packageName com.github.paohaijiao.mysql
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/1/31
 */


public class OracleTableBuilder extends AbstractTableBuilder {

    @Override
    protected String getCreateTableKeyword() {
        return "CREATE TABLE";
    }

    @Override
    protected String getNotNullKeyword() {
        return "NOT NULL";
    }

    @Override
    protected String getPrimaryKeyKeyword() {
        return "CONSTRAINT PK_%s PRIMARY KEY";
    }

    @Override
    protected String getAutoIncrementKeyword() {
        return "GENERATED ALWAYS AS IDENTITY";
    }

    @Override
    protected String getDefaultKeyword() {
        return "DEFAULT";
    }

    @Override
    protected String getCommentKeyword() {
        return "";
    }

    @Override
    protected String getUniqueKeyword() {
        return "CONSTRAINT";
    }

    @Override
    protected String getIndexKeyword() {
        return "";
    }

    @Override
    protected String getStatementTerminator() {
        return ";";
    }

    @Override
    public String buildIndexDefinition(JQuickIndexDefinition index) {
        return "";//to doracle的索引通常单独创建不在建表语句里面
    }

    @Override
    public String buildTableOptions(Map<String, Object> tableOptions) {
        return "";
    }

}