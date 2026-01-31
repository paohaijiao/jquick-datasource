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
package com.github.paohaijiao.factory;

import com.github.paohaijiao.abs.TableBuilder;
import com.github.paohaijiao.ddl.dialect.JQuickDatabaseDialect;
import com.github.paohaijiao.mysql.MySQLTableBuilder;
import com.github.paohaijiao.mysql.OracleTableBuilder;
import com.github.paohaijiao.mysql.PostgreSQLTableBuilder;

/**
 * packageName com.github.paohaijiao.factory
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/1/31
 */
public class TableBuilderFactory {

    public static TableBuilder createBuilder(JQuickDatabaseDialect dialect) {
        switch (dialect) {
            case MYSQL:
                return new MySQLTableBuilder();
            case POSTGRESQL:
                return new PostgreSQLTableBuilder();
            case ORACLE:
                return new OracleTableBuilder();
            default:
                throw new IllegalArgumentException("Unsupported database dialect: " + dialect);
        }
    }
}
