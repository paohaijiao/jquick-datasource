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

import com.github.paohaijiao.config.JQuickPoolConfig;
import com.github.paohaijiao.connector.JQuickDataSourceConnector;
import com.github.paohaijiao.console.JConsole;
import com.github.paohaijiao.dialect.JQuickSQLDialect;
import com.github.paohaijiao.pool.JQuickConnectionPool;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 连接池管理器
 * 管理多个数据库连接池实例
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/17
 */
public class JQuickPoolManager implements AutoCloseable {

    private static final JConsole console = new JConsole();
    /**
     * 默认连接池名称
     */
    private static final String DEFAULT_POOL = "default";
    private static volatile JQuickPoolManager instance;
    /**
     * 连接池映射表
     */
    private final Map<String, JQuickConnectionPool> pools;

    private JQuickPoolManager() {
        this.pools = new ConcurrentHashMap<>();
    }

    public static JQuickPoolManager getInstance() {
        if (instance == null) {
            synchronized (JQuickPoolManager.class) {
                if (instance == null) {
                    instance = new JQuickPoolManager();
                }
            }
        }
        return instance;
    }

    /**
     * 创建连接池
     */
    public JQuickConnectionPool createPool(JQuickDataSourceConnector connector, JQuickSQLDialect dialect) {
        return createPool(connector, dialect, DEFAULT_POOL, JQuickPoolConfig.defaultConfig());
    }

    /**
     * 创建连接池
     */
    public JQuickConnectionPool createPool(JQuickDataSourceConnector connector, JQuickSQLDialect dialect, JQuickPoolConfig config) {
        return createPool(connector, dialect, DEFAULT_POOL, config);
    }

    /**
     * 创建连接池
     */
    public JQuickConnectionPool createPool(JQuickDataSourceConnector connector, JQuickSQLDialect dialect, String poolName) {
        return createPool(connector, dialect, poolName, JQuickPoolConfig.defaultConfig());
    }

    /**
     * 创建连接池
     */
    public JQuickConnectionPool createPool(JQuickDataSourceConnector connector, JQuickSQLDialect dialect,
                                           String poolName, JQuickPoolConfig config) {
        if (pools.containsKey(poolName)) {
            console.warn("Pool already exists: " + poolName + ", returning existing pool");
            return pools.get(poolName);
        }

        config.setPoolName(poolName);
        JQuickConnectionPool pool = new JQuickConnectionPool(connector, dialect, config);
        pools.put(poolName, pool);
        console.info("Created connection pool: " + poolName);
        return pool;
    }

    /**
     * 根据连接配置创建连接池（自动检测方言）
     */
    public JQuickConnectionPool createPool(JQuickDataSourceConnector connector, String poolName, JQuickPoolConfig config) {
        JQuickDatabaseTypeManager manager = JQuickDatabaseTypeManager.getInstance();
        JQuickSQLDialect dialect;

        if (connector.getType() != null && !connector.getType().isEmpty()) {
            dialect = manager.getDialect(connector.getType());
        } else if (connector.getUrl() != null && !connector.getUrl().isEmpty()) {
            dialect = manager.getDialectByJdbcUrl(connector.getUrl());
        } else {
            throw new IllegalArgumentException("Cannot detect database type from connector");
        }

        if (dialect == null) {
            throw new IllegalArgumentException("No dialect found for database type");
        }

        return createPool(connector, dialect, poolName, config);
    }

    /**
     * 获取连接池
     */
    public JQuickConnectionPool getPool(String poolName) {
        return pools.get(poolName);
    }

    /**
     * 获取默认连接池
     */
    public JQuickConnectionPool getDefaultPool() {
        return pools.get(DEFAULT_POOL);
    }

    /**
     * 从默认连接池获取连接
     */
    public Connection getConnection() throws SQLException {
        JQuickConnectionPool pool = getDefaultPool();
        if (pool == null) {
            throw new SQLException("No default connection pool available");
        }
        return pool.getConnection();
    }

    /**
     * 从指定连接池获取连接
     */
    public Connection getConnection(String poolName) throws SQLException {
        JQuickConnectionPool pool = getPool(poolName);
        if (pool == null) {
            throw new SQLException("Connection pool not found: " + poolName);
        }
        return pool.getConnection();
    }

    /**
     * 关闭连接池
     */
    public void closePool(String poolName) {
        JQuickConnectionPool pool = pools.remove(poolName);
        if (pool != null) {
            pool.close();
            console.info("Closed connection pool: " + poolName);
        }
    }

    /**
     * 关闭所有连接池
     */
    @Override
    public void close() {
        for (Map.Entry<String, JQuickConnectionPool> entry : pools.entrySet()) {
            entry.getValue().close();
            console.info("Closed connection pool: " + entry.getKey());
        }
        pools.clear();
        console.info("All connection pools closed");
    }

    /**
     * 获取所有连接池的统计信息
     */
    public Map<String, Map<String, Object>> getAllStatistics() {
        Map<String, Map<String, Object>> allStats = new ConcurrentHashMap<>();
        for (Map.Entry<String, JQuickConnectionPool> entry : pools.entrySet()) {
            allStats.put(entry.getKey(), entry.getValue().getStatistics());
        }
        return allStats;
    }

    /**
     * 打印所有连接池统计信息
     */
    public void printAllStatistics() {
        console.info("========== All Connection Pools Statistics ==========");
        for (Map.Entry<String, JQuickConnectionPool> entry : pools.entrySet()) {
            console.info("Pool: " + entry.getKey());
            entry.getValue().printStatistics();
        }
        console.info("====================================================");
    }

    /**
     * 刷新指定连接池
     */
    public void refreshPool(String poolName) {
        JQuickConnectionPool pool = getPool(poolName);
        if (pool != null) {
            pool.refresh();
        }
    }

    /**
     * 刷新所有连接池
     */
    public void refreshAllPools() {
        for (JQuickConnectionPool pool : pools.values()) {
            pool.refresh();
        }
    }

    /**
     * 检查连接池是否存在
     */
    public boolean containsPool(String poolName) {
        return pools.containsKey(poolName);
    }

    /**
     * 获取所有连接池名称
     */
    public java.util.Set<String> getPoolNames() {
        return pools.keySet();
    }
}
