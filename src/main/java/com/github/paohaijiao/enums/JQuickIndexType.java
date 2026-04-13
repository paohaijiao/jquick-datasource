package com.github.paohaijiao.enums;

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

/**
 * 索引类型枚举
 * 定义数据库索引的不同类型
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/13
 */
public enum JQuickIndexType {
    /**
     * B-Tree 索引（B-树索引）
     * 最常用的索引类型，适用于全键值、键值范围或键值前缀查找
     * 支持 =, >, >=, <, <=, BETWEEN, LIKE 'pattern%' 等操作符
     * 默认索引类型
     */
    BTREE,

    /**
     * Hash 索引（哈希索引）
     * 基于哈希表实现，只适用于精确匹配查找（= 或 <=>）
     * 不支持范围查询，查询速度快（O(1) 时间复杂度）
     * 主要用于 Memory 存储引擎
     */
    HASH,

    /**
     * Full-Text 索引（全文索引）
     * 用于全文搜索，支持对文本内容进行分词搜索
     * 适用于 LIKE '%keyword%' 或 MATCH AGAINST 查询
     * 主要用于 MyISAM 和 InnoDB 存储引擎（MySQL 5.6+）
     */
    FULLTEXT,

    /**
     * Spatial 索引（空间索引）
     * 用于地理空间数据类型（如 POINT, LINESTRING, POLYGON）
     * 支持空间相关查询（如包含、相交、距离等）
     * 主要用于 MyISAM 和 InnoDB 存储引擎（MySQL 5.7+）
     */
    SPATIAL,

    /**
     * Bitmap 索引（位图索引）
     * 使用位图结构存储索引，适合低基数（distinct 值少）的列
     * 主要用于 Oracle、PostgreSQL 等数据库
     * 不适合频繁更新的表
     */
    BITMAP,

    /**
     * GiST 索引（通用搜索树索引）
     * PostgreSQL 特有的索引类型，支持几何数据类型和全文搜索
     * 可用于实现 R-tree（空间索引）和全文检索
     */
    GIST,

    /**
     * GIN 索引（广义倒排索引）
     * PostgreSQL 特有的索引类型，适用于包含多个键值的数据类型
     * 如数组、JSON、全文检索等
     */
    GIN,

    /**
     * 聚簇索引
     * 数据行的物理顺序与索引顺序相同
     * 每个表只能有一个聚簇索引
     * 例如 MySQL InnoDB 的主键就是聚簇索引
     */
    CLUSTERED,

    /**
     * 非聚簇索引
     * 索引顺序与数据行物理顺序不同
     * 每个表可以有多个非聚簇索引
     */
    NON_CLUSTERED
}
