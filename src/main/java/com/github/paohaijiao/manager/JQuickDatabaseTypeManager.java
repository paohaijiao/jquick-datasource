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
package com.github.paohaijiao.manager;

/**
 * packageName com.github.paohaijiao.manager
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/13
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
import com.github.paohaijiao.console.JConsole;
import com.github.paohaijiao.dialect.JQuickSQLDialect;
import com.github.paohaijiao.provider.JQuickSQLDialectProvider;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.logging.Logger;

/**
 * 数据库类型管理器
 * 基于 HashMap 存储方言，支持 ServiceLoader 动态加载
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/13
 */
public class JQuickDatabaseTypeManager {

    private JConsole console=new JConsole();

    private static volatile JQuickDatabaseTypeManager instance;

    private final Map<String, JQuickSQLDialect> dialectMap;

    private final Map<String, JQuickSQLDialectProvider> providerMap;

    private final Map<String, String> urlPatternMap;

    private final ReentrantReadWriteLock lock;

    /**
     * 私有构造函数，初始化管理器
     */
    private JQuickDatabaseTypeManager() {
        this.dialectMap = new ConcurrentHashMap<>();
        this.providerMap = new ConcurrentHashMap<>();
        this.urlPatternMap = new ConcurrentHashMap<>();
        this.lock = new ReentrantReadWriteLock();
        initDefaultUrlPatterns();
        loadDialectsFromServiceLoader();
    }

    /**
     * 获取单例实例
     * @return DatabaseTypeManager 实例
     */
    public static JQuickDatabaseTypeManager getInstance() {
        if (instance == null) {
            synchronized (JQuickDatabaseTypeManager.class) {
                if (instance == null) {
                    instance = new JQuickDatabaseTypeManager();
                }
            }
        }
        return instance;
    }

    /**
     * 初始化默认的 JDBC URL 检测规则
     */
    private void initDefaultUrlPatterns() {
        urlPatternMap.put("jdbc:mysql:", "mysql");
        urlPatternMap.put("jdbc:mariadb:", "mariadb");
        urlPatternMap.put("jdbc:postgresql:", "postgresql");
        urlPatternMap.put("jdbc:oracle:", "oracle");
        urlPatternMap.put("jdbc:sqlserver:", "sqlserver");
        urlPatternMap.put("jdbc:h2:", "h2");
        urlPatternMap.put("jdbc:h2:mem:", "h2");
        urlPatternMap.put("jdbc:hsqldb:", "hsqldb");
        urlPatternMap.put("jdbc:derby:", "derby");
        urlPatternMap.put("jdbc:sqlite:", "sqlite");
        urlPatternMap.put("jdbc:clickhouse:", "clickhouse");
        urlPatternMap.put("jdbc:doris:", "doris");
        urlPatternMap.put("jdbc:starrocks:", "starrocks");
    }

    /**
     * 通过 ServiceLoader 动态加载所有方言提供者
     */
    private void loadDialectsFromServiceLoader() {
        ServiceLoader<JQuickSQLDialectProvider> loader = ServiceLoader.load(JQuickSQLDialectProvider.class);
        List<JQuickSQLDialectProvider> providers = new ArrayList<>();
        for (JQuickSQLDialectProvider provider : loader) {
            providers.add(provider);
            console.info("发现方言提供者: " + provider.getClass().getName() + ", 数据库类型: " + provider.getDatabaseType());
        }
        providers.sort(Comparator.comparingInt(JQuickSQLDialectProvider::getPriority));
        for (JQuickSQLDialectProvider provider : providers) {
            registerDialectProvider(provider);
        }
        console.info("通过 ServiceLoader 加载了 " + providers.size() + " 个方言提供者");
    }

    /**
     * 注册方言提供者
     * @param provider 方言提供者
     */
    public void registerDialectProvider(JQuickSQLDialectProvider provider) {
        String dbType = provider.getDatabaseType().toLowerCase();
        lock.writeLock().lock();
        try {
            providerMap.put(dbType, provider);
            console.info("注册方言提供者: " + dbType + " -> " + provider.getClass().getName());
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 注册方言实例
     * @param databaseType 数据库类型
     * @param dialect 方言实例
     */
    public void registerDialect(String databaseType, JQuickSQLDialect dialect) {
        if (databaseType == null || databaseType.trim().isEmpty()) {
            throw new IllegalArgumentException("数据库类型不能为空");
        }
        if (dialect == null) {
            throw new IllegalArgumentException("方言实例不能为空");
        }
        String dbType = databaseType.toLowerCase();
        lock.writeLock().lock();
        try {
            dialectMap.put(dbType, dialect);
            console.info("注册方言: " + dbType + " -> " + dialect.getClass().getName());
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 批量注册方言
     * @param dialects 方言映射
     */
    public void registerDialects(Map<String, JQuickSQLDialect> dialects) {
        if (dialects == null || dialects.isEmpty()) {
            return;
        }
        lock.writeLock().lock();
        try {
            for (Map.Entry<String, JQuickSQLDialect> entry : dialects.entrySet()) {
                String dbType = entry.getKey().toLowerCase();
                dialectMap.put(dbType, entry.getValue());
                console.info("注册方言: " + dbType + " -> " + entry.getValue().getClass().getName());
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 获取方言
     * @param databaseType 数据库类型（不区分大小写）
     * @return SQLDialect 实例，如果不存在则返回 null
     */
    public JQuickSQLDialect getDialect(String databaseType) {
        if (databaseType == null || databaseType.trim().isEmpty()) {
            return null;
        }
        String dbType = databaseType.toLowerCase();
        lock.readLock().lock();
        try {
            JQuickSQLDialect dialect = dialectMap.get(dbType);
            if (dialect != null) {
                return dialect;
            }
        } finally {
            lock.readLock().unlock();
        }
        lock.writeLock().lock();
        try {
            if (dialectMap.containsKey(dbType)) {
                return dialectMap.get(dbType);
            }
            JQuickSQLDialectProvider provider = providerMap.get(dbType);
            if (provider != null) {
                JQuickSQLDialect newDialect = provider.createDialect();
                if (newDialect != null) {
                    dialectMap.put(dbType, newDialect);
                    console.info("通过提供者创建方言: " + dbType);
                    return newDialect;
                }
            }
        } finally {
            lock.writeLock().unlock();
        }
        console.warn("未找到数据库类型的方言: " + databaseType);
        return null;
    }

    /**
     * 获取方言，如果不存在则返回默认方言
     * @param databaseType 数据库类型
     * @param defaultDialect 默认方言
     * @return SQLDialect 实例
     */
    public JQuickSQLDialect getDialectOrDefault(String databaseType, JQuickSQLDialect defaultDialect) {
        JQuickSQLDialect dialect = getDialect(databaseType);
        return dialect != null ? dialect : defaultDialect;
    }

    /**
     * 根据 JDBC URL 自动检测数据库类型并获取方言
     * @param jdbcUrl JDBC URL
     * @return SQLDialect 实例，如果无法检测则返回 null
     */
    public JQuickSQLDialect getDialectByJdbcUrl(String jdbcUrl) {
        if (jdbcUrl == null || jdbcUrl.trim().isEmpty()) {
            return null;
        }
        String lowerUrl = jdbcUrl.toLowerCase();
        for (Map.Entry<String, String> entry : urlPatternMap.entrySet()) {
            if (lowerUrl.contains(entry.getKey())) {
                String dbType = entry.getValue();
                JQuickSQLDialect dialect = getDialect(dbType);
                if (dialect != null) {
                    console.info("通过 JDBC URL 检测到数据库类型: " + dbType + ", URL: " + jdbcUrl);
                    return dialect;
                }
            }
        }
        lock.readLock().lock();
        try {
            for (Map.Entry<String, JQuickSQLDialectProvider> entry : providerMap.entrySet()) {
                JQuickSQLDialectProvider provider = entry.getValue();
                if (provider.supports(jdbcUrl)) {
                    JQuickSQLDialect dialect = getDialect(entry.getKey());
                    if (dialect != null) {
                        console.info("通过提供者检测到数据库类型: " + entry.getKey() + ", URL: " + jdbcUrl);
                        return dialect;
                    }
                }
            }
        } finally {
            lock.readLock().unlock();
        }
        console.warn("无法从 JDBC URL 检测数据库类型: " + jdbcUrl);
        return null;
    }

    /**
     * 检查是否支持指定的数据库类型
     * @param databaseType 数据库类型
     * @return 是否支持
     */
    public boolean supports(String databaseType) {
        if (databaseType == null || databaseType.trim().isEmpty()) {
            return false;
        }
        String dbType = databaseType.toLowerCase();
        lock.readLock().lock();
        try {
            return dialectMap.containsKey(dbType) || providerMap.containsKey(dbType);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 移除方言
     * @param databaseType 数据库类型
     * @return 被移除的方言，如果不存在则返回 null
     */
    public JQuickSQLDialect removeDialect(String databaseType) {
        if (databaseType == null || databaseType.trim().isEmpty()) {
            return null;
        }
        String dbType = databaseType.toLowerCase();
        lock.writeLock().lock();
        try {
            JQuickSQLDialect removed = dialectMap.remove(dbType);
            if (removed != null) {
                console.info("移除方言: " + dbType);
            }
            return removed;
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 获取所有已注册的数据库类型
     * @return 数据库类型集合（不可修改）
     */
    public Set<String> getRegisteredDatabaseTypes() {
        lock.readLock().lock();
        try {
            Set<String> allTypes = new HashSet<>();
            allTypes.addAll(dialectMap.keySet());
            allTypes.addAll(providerMap.keySet());
            return Collections.unmodifiableSet(allTypes);
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 获取所有已实例化的方言
     * @return 方言映射（不可修改）
     */
    public Map<String, JQuickSQLDialect> getAllDialects() {
        lock.readLock().lock();
        try {
            return Collections.unmodifiableMap(new HashMap<>(dialectMap));
        } finally {
            lock.readLock().unlock();
        }
    }

    /**
     * 清空所有注册的方言（不包括通过 ServiceLoader 加载的提供者）
     */
    public void clear() {
        lock.writeLock().lock();
        try {
            dialectMap.clear();
            console.info("清空所有方言缓存");
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 重新加载所有通过 ServiceLoader 提供的方言
     */
    public void reload() {
        lock.writeLock().lock();
        try {
            dialectMap.clear();
            providerMap.clear();
            loadDialectsFromServiceLoader();
            console.info("重新加载所有方言完成");
        } finally {
            lock.writeLock().unlock();
        }
    }

    /**
     * 获取管理器统计信息
     * @return 统计信息
     */
    public Map<String, Object> getStatistics() {
        lock.readLock().lock();
        try {
            Map<String, Object> stats = new HashMap<>();
            stats.put("cachedDialects", dialectMap.size());
            stats.put("registeredProviders", providerMap.size());
            stats.put("databaseTypes", getRegisteredDatabaseTypes());
            stats.put("urlPatterns", new HashMap<>(urlPatternMap));
            return stats;
        } finally {
            lock.readLock().unlock();
        }
    }
}
