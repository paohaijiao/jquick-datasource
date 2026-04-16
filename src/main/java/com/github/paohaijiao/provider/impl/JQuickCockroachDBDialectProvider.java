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

import com.github.paohaijiao.dialect.JQuickSQLDialect;
import com.github.paohaijiao.dialect.impl.JQuickCockroachDBDialect;
import com.github.paohaijiao.provider.JQuickSQLDialectProvider;

/**
 * CockroachDB 方言提供者
 * CockroachDB 兼容 PostgreSQL 协议
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/16
 */
public class JQuickCockroachDBDialectProvider implements JQuickSQLDialectProvider {

    @Override
    public String getDatabaseType() {
        return "cockroachdb";
    }

    @Override
    public JQuickSQLDialect createDialect() {
        return new JQuickCockroachDBDialect();
    }

    @Override
    public boolean supports(String jdbcUrl) {
        if (jdbcUrl == null) {
            return false;
        }
        if (jdbcUrl.startsWith("jdbc:postgresql://")) {
            String lowerUrl = jdbcUrl.toLowerCase();
            return lowerUrl.contains("cockroach") || lowerUrl.contains("crdb");
        }
        return false;
    }

    @Override
    public int getPriority() {
        return 150;
    }
}
