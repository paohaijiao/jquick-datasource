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
package com.github.paohaijiao.provider.impl;

import com.github.paohaijiao.dialect.JQuickSQLDialect;
import com.github.paohaijiao.dialect.impl.JQuickAccessDialect;
import com.github.paohaijiao.dialect.impl.JQuickKingbaseESDialect;
import com.github.paohaijiao.provider.JQuickSQLDialectProvider;

/**
 * packageName com.github.paohaijiao.provider.impl
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/14
 */
public class JQuickAccessDialectProvider implements JQuickSQLDialectProvider {
    @Override
    public String getDatabaseType() {
        return "access";
    }

    @Override
    public JQuickSQLDialect createDialect() {
        return new JQuickAccessDialect();
    }
    public  boolean supports(String jdbcUrl) {
        if (jdbcUrl == null) {
            return false;
        }
        return jdbcUrl.startsWith("jdbc:ucanaccess:");
    }
}
