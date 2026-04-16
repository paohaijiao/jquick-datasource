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

/**
 * packageName com.github.paohaijiao.provider.impl
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/16
 */
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
import com.github.paohaijiao.dialect.impl.JQuickRedshiftDialect;
import com.github.paohaijiao.provider.JQuickSQLDialectProvider;

/**
 * Amazon Redshift 方言提供者
 * Redshift 兼容 PostgreSQL 协议
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/16
 */
public class JQuickRedshiftDialectProvider implements JQuickSQLDialectProvider {

    @Override
    public String getDatabaseType() {
        return "redshift";
    }

    @Override
    public JQuickSQLDialect createDialect() {
        return new JQuickRedshiftDialect();
    }

    @Override
    public boolean supports(String jdbcUrl) {
        if (jdbcUrl == null) {
            return false;
        }
        String lowerUrl = jdbcUrl.toLowerCase();
        if (lowerUrl.startsWith("jdbc:redshift:")) {
            return true;
        }
        if (lowerUrl.startsWith("jdbc:postgresql://")) {
            return lowerUrl.contains("redshift") || lowerUrl.contains("amazonaws");
        }
        return false;
    }

    @Override
    public int getPriority() {
        return 150;
    }
}
