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
package com.github.paohaijiao.extra;

import com.github.paohaijiao.enums.JQuickIndexType;
import lombok.Data;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * packageName com.github.paohaijiao.extra
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/13
 */
@Data
public class JQuickIndexDefinition {

    private String indexName;

    private List<String> columns;

    private boolean unique;

    private String type; // BTREE, HASH, FULLTEXT

    private String comment;
    private String whereCondition;

    private Map<String, Object> extensions = new HashMap<>();
    /**
     * 添加扩展属性
     */
    public JQuickIndexDefinition addExtension(String key, Object value) {
        if (extensions == null) {
            extensions = new HashMap<>();
        }
        extensions.put(key, value);
        return this;
    }

    /**
     * 获取扩展属性
     */
    @SuppressWarnings("unchecked")
    public <T> T getExtension(String key) {
        if (extensions == null) {
            return null;
        }
        return (T) extensions.get(key);
    }

    /**
     * 检查是否有扩展属性
     */
    public boolean hasExtension(String key) {
        return extensions != null && extensions.containsKey(key);
    }

}
