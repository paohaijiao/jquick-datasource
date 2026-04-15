package com.github.paohaijiao.impl;
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

import com.github.paohaijiao.column.JQuickColumnDefinition;
import com.github.paohaijiao.dataType.JQuickDataType;
import com.github.paohaijiao.dataType.JQuickDataTypeConverter;
import com.github.paohaijiao.dataType.enums.JQuickDataTypeFamily;
import com.github.paohaijiao.dialect.JQuickAbsSQLDialect;
import com.github.paohaijiao.row.JQuickRow;
import com.github.paohaijiao.table.JQuickTableDefinition;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * TiDB 方言实现
 * TiDB 高度兼容 MySQL，继承 MySQL 方言并添加 TiDB 特有功能
 * <p>
 * TiDB 特有功能：
 * - 分布式事务
 * - 热点分裂
 * - 列式存储（TiFlash）
 * - 自动统计信息
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/15
 */
public class JQuickTiDBDialect extends JQuickMySQLDialect {

    @Override
    protected String getQuoteKeyWord() {
        return "`";
    }

    @Override
    protected void appendTableOptions(StringBuilder sql, JQuickTableDefinition table) {
        super.appendTableOptions(sql, table);
        // TiDB 特有选项
        if (table.getExtensions() != null) {
            // 分片索引（热点分裂）
            if (table.getExtensions().containsKey("shardRowIdBits")) {
                int shardBits = (Integer) table.getExtensions().get("shardRowIdBits");
                sql.append(" SHARD_ROW_ID_BITS = ").append(shardBits);
            }

            // TiFlash 副本
            if (table.getExtensions().containsKey("tiflashReplica")) {
                int replica = (Integer) table.getExtensions().get("tiflashReplica");
                sql.append(" TIFLASH_REPLICA ").append(replica);
            }

            // 自动统计信息
            if (table.getExtensions().containsKey("autoStats")) {
                Boolean autoStats = (Boolean) table.getExtensions().get("autoStats");
                if (autoStats != null && autoStats) {
                    sql.append(" STATS_AUTO_RECALC = ON");
                }
            }

            // 采样率
            if (table.getExtensions().containsKey("statsSamplePages")) {
                int samplePages = (Integer) table.getExtensions().get("statsSamplePages");
                sql.append(" STATS_SAMPLE_PAGES = ").append(samplePages);
            }
        }
    }

    /**
     * 构建添加 TiFlash 副本语句
     */
    public String buildAddTiFlashReplica(String tableName, int replicaCount) {
        return "ALTER TABLE " + quoteIdentifier(null, tableName) + " SET TIFLASH_REPLICA " + replicaCount;
    }

    /**
     * 构建查询 TiFlash 副本状态语句
     */
    public String buildShowTiFlashReplicaStatus(String tableName) {
        return "SELECT * FROM information_schema.tiflash_replica WHERE TABLE_NAME = '" + tableName + "'";
    }

    /**
     * 构建分析表语句（TiDB 特有）
     */
    public String buildAnalyzeTableWithOptions(String tableName, int samplePages) {
        return "ANALYZE TABLE " + quoteIdentifier(null, tableName) + " SAMPLE " + samplePages + " PAGES";
    }
}


