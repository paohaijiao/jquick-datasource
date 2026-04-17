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
package com.github.paohaijiao.pool;

import com.github.paohaijiao.config.JQuickPoolConfig;
import com.github.paohaijiao.connection.JQuickPooledConnection;
import com.github.paohaijiao.connector.JQuickDataSourceConnector;
import com.github.paohaijiao.console.JConsole;
import com.github.paohaijiao.dialect.JQuickSQLDialect;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/**
 * 数据库连接池实现
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/17
 */
public class JQuickConnectionPool implements AutoCloseable {

    private static final JConsole console = new JConsole();
    /**
     * 空闲连接队列
     */
    private final BlockingQueue<JQuickPooledConnection> idleConnections;

    /**
     * 活跃连接集合
     */
    private final Set<JQuickPooledConnection> activeConnections;

    /**
     * 配置
     */
    private final JQuickPoolConfig config;

    /**
     * 数据源连接配置
     */
    private final JQuickDataSourceConnector connector;

    /**
     * SQL方言
     */
    private final JQuickSQLDialect dialect;

    /**
     * 锁
     */
    private final ReentrantLock lock = new ReentrantLock();

    /**
     * 等待条件
     */
    private final Condition connectionAvailable = lock.newCondition();
    /**
     * 当前活跃连接数
     */
    private final AtomicLong activeCount = new AtomicLong(0);
    /**
     * 总连接数
     */
    private final AtomicLong totalCount = new AtomicLong(0);
    /**
     * 等待获取连接的线程数
     */
    private final AtomicLong waitingThreads = new AtomicLong(0);
    /**
     * 累计借出连接数
     */
    private final AtomicLong totalBorrowed = new AtomicLong(0);
    /**
     * 累计归还连接数
     */
    private final AtomicLong totalReturned = new AtomicLong(0);
    /**
     * 累计创建连接数
     */
    private final AtomicLong totalCreated = new AtomicLong(0);
    /**
     * 累计销毁连接数
     */
    private final AtomicLong totalDestroyed = new AtomicLong(0);
    /**
     * 累计超时次数
     */
    private final AtomicLong totalTimeoutCount = new AtomicLong(0);
    /**
     * 连接池状态
     */
    private volatile boolean isRunning = true;
    /**
     * 空闲连接检查定时器
     */
    private ScheduledExecutorService houseKeeper;

    /**
     * 连接泄漏检测定时器
     */
    private ScheduledExecutorService leakDetector;

    /**
     * 创建连接的线程池
     */
    private ExecutorService connectionCreator;

    public JQuickConnectionPool(JQuickDataSourceConnector connector, JQuickSQLDialect dialect) {
        this(connector, dialect, JQuickPoolConfig.defaultConfig());
    }

    public JQuickConnectionPool(JQuickDataSourceConnector connector, JQuickSQLDialect dialect, JQuickPoolConfig config) {
        this.connector = connector;
        this.dialect = dialect;
        this.config = config;
        this.idleConnections = new LinkedBlockingQueue<>();
        this.activeConnections = ConcurrentHashMap.newKeySet();
        initialize();
    }

    /**
     * 初始化连接池
     */
    private void initialize() {
        try {
            String driverClass = dialect.getDriverClass(connector);
            if (driverClass != null && !driverClass.isEmpty()) {
                Class.forName(driverClass);
                console.info("Loaded driver: " + driverClass);
            }
        } catch (ClassNotFoundException e) {
            console.error("Failed to load driver", e);
        }
        for (int i = 0; i < config.getMinIdle(); i++) {
            createConnection();
        }
        if (config.getIdleCheckInterval() > 0) {
            houseKeeper = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, config.getPoolName() + "-HouseKeeper");
                t.setDaemon(true);
                return t;
            });
            houseKeeper.scheduleAtFixedRate(this::houseKeeping,
                    config.getIdleCheckInterval(),
                    config.getIdleCheckInterval(),
                    TimeUnit.MILLISECONDS);
        }
        if (config.isLeakDetectionEnabled() && config.getLeakDetectionThreshold() > 0) {
            leakDetector = Executors.newSingleThreadScheduledExecutor(r -> {
                Thread t = new Thread(r, config.getPoolName() + "-LeakDetector");
                t.setDaemon(true);
                return t;
            });
            leakDetector.scheduleAtFixedRate(this::detectLeaks,
                    config.getLeakDetectionThreshold(),
                    config.getLeakDetectionThreshold(),
                    TimeUnit.MILLISECONDS);
        }
        connectionCreator = Executors.newCachedThreadPool(r -> {
            Thread t = new Thread(r, config.getPoolName() + "-ConnectionCreator");
            t.setDaemon(true);
            return t;
        });
        console.info("Connection pool initialized: " + config);
    }

    /**
     * 创建新连接
     */
    private JQuickPooledConnection createConnection() {
        try {
            Connection conn = dialect.getConnection(connector);
            if (conn == null) {
                console.error("Failed to create connection");
                return null;
            }
            conn.setAutoCommit(true);
            conn.setNetworkTimeout(Executors.newSingleThreadExecutor(), config.getConnectionTimeout());
            JQuickPooledConnection pooledConn = new JQuickPooledConnection(conn, this);
            idleConnections.offer(pooledConn);
            totalCount.incrementAndGet();
            totalCreated.incrementAndGet();
            console.debug("Created new connection, total: " + totalCount.get());
            return pooledConn;
        } catch (SQLException e) {
            console.error("Failed to create connection", e);
            return null;
        }
    }

    /**
     * 异步创建连接
     */
    private CompletableFuture<JQuickPooledConnection> createConnectionAsync() {
        return CompletableFuture.supplyAsync(this::createConnection, connectionCreator);
    }

    /**
     * 获取连接
     */
    public Connection getConnection() throws SQLException {
        return getConnection(config.getMaxWait());
    }

    /**
     * 获取连接（带超时）
     */
    public Connection getConnection(long timeout) throws SQLException {
        if (!isRunning) {
            throw new SQLException("Connection pool is closed");
        }
        waitingThreads.incrementAndGet();
        long startTime = System.currentTimeMillis();
        long remainingTimeout = timeout;
        try {
            while (true) {
                JQuickPooledConnection conn = idleConnections.poll();
                if (conn != null) {
                    if (validateConnection(conn)) {
                        return borrowConnection(conn);
                    } else {
                        destroyConnection(conn);
                        continue;
                    }
                }
                // 检查是否可以创建新连接
                if (totalCount.get() < config.getMaxActive()) {
                    // 异步创建新连接
                    CompletableFuture<JQuickPooledConnection> future = createConnectionAsync();
                    try {
                        conn = future.get(remainingTimeout, TimeUnit.MILLISECONDS);
                        if (validateConnection(conn)) {
                            return borrowConnection(conn);
                        }
                    } catch (TimeoutException e) {
                        future.cancel(true);
                        throw new SQLException("Timeout waiting for connection creation");
                    } catch (Exception e) {
                        console.error("Failed to create connection", e);
                    }
                    continue;
                }
                if (remainingTimeout <= 0) {
                    totalTimeoutCount.incrementAndGet();
                    throw new SQLException("Timeout waiting for connection");
                }

                lock.lock();
                try {
                    long waitStart = System.currentTimeMillis();
                    connectionAvailable.await(remainingTimeout, TimeUnit.MILLISECONDS);
                    remainingTimeout -= (System.currentTimeMillis() - waitStart);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                    throw new SQLException("Interrupted while waiting for connection", e);
                } finally {
                    lock.unlock();
                }
            }
        } finally {
            waitingThreads.decrementAndGet();
        }
    }

    /**
     * 验证连接
     */
    private boolean validateConnection(JQuickPooledConnection conn) {
        if (conn == null) {
            return false;
        }
        try {
            if (conn.isClosed()) { // 检查连接是否已关闭
                return false;
            }
            if (conn.isExpired(config.getMaxLifetime())) {// 检查连接是否过期
                console.debug("Connection expired: " + conn.getCreatedAt());
                return false;
            }
            // 执行验证查询
            if (config.isTestOnBorrow() || (config.isTestWhileIdle() && !conn.isValidated())) {
                boolean valid;
                if (config.getValidationQuery() != null && !config.getValidationQuery().isEmpty()) {
                    valid = conn.testValidationQuery(config.getValidationQuery(), config.getValidationTimeout());
                } else {
                    valid = conn.isValid(config.getValidationTimeout());
                }
                if (!valid) {
                    console.debug("Connection validation failed");
                    return false;
                }
                conn.setValidated(true);
            }
            return true;
        } catch (Exception e) {
            console.debug("Connection validation error: " + e.getMessage());
            return false;
        }
    }

    /**
     * 借出连接
     */
    private Connection borrowConnection(JQuickPooledConnection conn) {
        activeConnections.add(conn);
        activeCount.incrementAndGet();
        totalBorrowed.incrementAndGet();
        conn.incrementBorrowCount();
        conn.setLastUsedAt(System.currentTimeMillis());
        conn.setBorrowedBy(Thread.currentThread());
        if (config.isLeakDetectionEnabled()) {
            conn.setBorrowStackTrace(Thread.currentThread().getStackTrace());
        }
        console.debug("Borrowed connection, active: " + activeCount.get() + ", idle: " + idleConnections.size());
        return conn;
    }

    /**
     * 归还连接
     */
    public void returnConnection(JQuickPooledConnection conn) {
        if (!isRunning) {
            destroyConnection(conn);
            return;
        }
        activeConnections.remove(conn);
        activeCount.decrementAndGet();
        totalReturned.incrementAndGet();
        conn.setLastUsedAt(System.currentTimeMillis());
        conn.setBorrowedBy(null);
        // 重置连接状态
        try {
            if (!conn.isClosed()) {
                if (!conn.getAutoCommit()) { // 回滚未提交的事务
                    conn.rollback();
                    conn.setAutoCommit(true);
                }
                conn.clearWarnings();
            }
        } catch (SQLException e) {
            console.warn("Failed to reset connection: " + e.getMessage());
            destroyConnection(conn);
            return;
        }
        if (validateConnection(conn)) {  // 检查连接是否仍然有效
            if (idleConnections.size() >= config.getMaxIdle()) {  // 检查空闲连接数是否超限
                destroyConnection(conn);
            } else {
                conn.setValidated(false);
                idleConnections.offer(conn);
                console.debug("Returned connection, active: " + activeCount.get() + ", idle: " + idleConnections.size());
            }
        } else {
            destroyConnection(conn);
        }
        lock.lock();
        try { // 通知等待的线程
            connectionAvailable.signal();
        } finally {
            lock.unlock();
        }
        ensureMinIdle();// 补充空闲连接
    }

    /**
     * 销毁连接
     */
    private void destroyConnection(JQuickPooledConnection conn) {
        try {
            conn.forceClose();
        } catch (SQLException e) {
            console.warn("Failed to close connection: " + e.getMessage());
        }
        totalCount.decrementAndGet();
        totalDestroyed.incrementAndGet();
        console.debug("Destroyed connection, total: " + totalCount.get());
    }

    /**
     * 确保最小空闲连接数
     */
    private void ensureMinIdle() {
        int toCreate = config.getMinIdle() - idleConnections.size();
        for (int i = 0; i < toCreate && totalCount.get() < config.getMaxActive(); i++) {
            createConnectionAsync();
        }
    }

    /**
     * 空闲连接维护
     */
    private void houseKeeping() {
        if (!isRunning) {
            return;
        }
        console.debug("Running housekeeping...");
        List<JQuickPooledConnection> toRemove = new ArrayList<>();
        for (JQuickPooledConnection conn : idleConnections) {// 清理空闲连接
            // 检查空闲超时
            if (idleConnections.size() > config.getMinIdle() && conn.isIdleTimeout(config.getMaxIdleTime())) {
                toRemove.add(conn);
            }
            // 检查过期
            else if (conn.isExpired(config.getMaxLifetime())) {
                toRemove.add(conn);
            }
            // 空闲验证
            else if (config.isTestWhileIdle()) {
                if (!validateConnection(conn)) {
                    toRemove.add(conn);
                }
            }
        }
        for (JQuickPooledConnection conn : toRemove) {
            idleConnections.remove(conn);
            destroyConnection(conn);
        }
        ensureMinIdle(); // 补充空闲连接
        console.debug("Housekeeping done, idle: " + idleConnections.size());
    }

    /**
     * 检测连接泄漏
     */
    private void detectLeaks() {
        if (!isRunning) {
            return;
        }
        long now = System.currentTimeMillis();
        long threshold = config.getLeakDetectionThreshold();
        for (JQuickPooledConnection conn : activeConnections) {
            long borrowedTime = now - conn.getLastUsedAt();
            if (borrowedTime > threshold) {
                long lastWarning = conn.getLastLeakedWarningAt();
                if (now - lastWarning > threshold) {
                    conn.setLastLeakedWarningAt(now);
                    console.warn("Potential connection leak detected! " + "Connection borrowed for " + borrowedTime + "ms, " + "borrowed by: " + conn.getBorrowedBy());
                    if (conn.getBorrowStackTrace() != null) {
                        console.warn("Borrow stack trace:");
                        for (StackTraceElement ste : conn.getBorrowStackTrace()) {
                            console.warn("  " + ste.toString());
                        }
                    }
                }
            }
        }
    }

    /**
     * 关闭连接池
     */
    @Override
    public void close() {
        if (!isRunning) {
            return;
        }
        isRunning = false;
        console.info("Closing connection pool...");
        if (houseKeeper != null) { // 关闭定时器
            houseKeeper.shutdown();
            try {
                houseKeeper.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (leakDetector != null) {
            leakDetector.shutdown();
            try {
                leakDetector.awaitTermination(5, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        if (connectionCreator != null) {
            connectionCreator.shutdown();
            try {
                connectionCreator.awaitTermination(10, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
            }
        }
        List<JQuickPooledConnection> allConnections = new ArrayList<>();
        allConnections.addAll(idleConnections);
        allConnections.addAll(activeConnections);
        for (JQuickPooledConnection conn : allConnections) {  // 关闭所有连接
            try {
                conn.forceClose();
            } catch (SQLException e) {
                console.error("Failed to close connection", e);
            }
        }
        idleConnections.clear();
        activeConnections.clear();
        console.info("Connection pool closed. Statistics: " + getStatistics());
    }

    /**
     * 获取连接池统计信息
     */
    public Map<String, Object> getStatistics() {
        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("poolName", config.getPoolName());
        stats.put("isRunning", isRunning);
        stats.put("totalConnections", totalCount.get());
        stats.put("activeConnections", activeCount.get());
        stats.put("idleConnections", idleConnections.size());
        stats.put("waitingThreads", waitingThreads.get());
        stats.put("totalBorrowed", totalBorrowed.get());
        stats.put("totalReturned", totalReturned.get());
        stats.put("totalCreated", totalCreated.get());
        stats.put("totalDestroyed", totalDestroyed.get());
        stats.put("totalTimeoutCount", totalTimeoutCount.get());
        stats.put("config", config.toString());
        return stats;
    }
    /**
     * 打印统计信息
     */
    public void printStatistics() {
        Map<String, Object> stats = getStatistics();
        console.info("========== Connection Pool Statistics ==========");
        for (Map.Entry<String, Object> entry : stats.entrySet()) {
            console.info(String.format("%-20s: %s", entry.getKey(), entry.getValue()));
        }
        console.info("================================================");
    }

    /**
     * 清空空闲连接
     */
    public void evictIdleConnections() {
        List<JQuickPooledConnection> toEvict = new ArrayList<>();
        idleConnections.drainTo(toEvict);
        for (JQuickPooledConnection conn : toEvict) {
            destroyConnection(conn);
        }
        console.info("Evicted " + toEvict.size() + " idle connections");
    }

    /**
     * 刷新连接池（销毁所有连接并重新创建）
     */
    public void refresh() {
        console.info("Refreshing connection pool...");
        evictIdleConnections();
        // 关闭所有活跃连接
        for (JQuickPooledConnection conn : activeConnections) {
            try {
                conn.forceClose();
                activeConnections.remove(conn);
                totalCount.decrementAndGet();
                totalDestroyed.incrementAndGet();
            } catch (SQLException e) {
                console.error("Failed to close active connection", e);
            }
        }
        activeCount.set(0);
        for (int i = 0; i < config.getMinIdle(); i++) {// 重建最小空闲连接
            createConnection();
        }

        console.info("Connection pool refreshed");
    }

    public JQuickPoolConfig getConfig() {
        return config;
    }

    public void logSql(String sql) {
        console.info("[SQL] " + sql);
    }
}
