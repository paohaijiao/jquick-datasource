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
package com.github.paohaijiao.config;

/**
 * 连接池配置类
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/17
 */
public class JQuickPoolConfig {

    /**
     * 最小空闲连接数
     */
    private int minIdle = 2;

    /**
     * 最大活跃连接数
     */
    private int maxActive = 20;

    /**
     * 最大空闲连接数
     */
    private int maxIdle = 10;

    /**
     * 最大等待时间（毫秒），-1表示无限等待
     */
    private long maxWait = 30000;

    /**
     * 连接超时时间（毫秒）
     */
    private int connectionTimeout = 30000;

    /**
     * 验证查询SQL
     */
    private String validationQuery = "SELECT 1";

    /**
     * 验证超时时间（秒）
     */
    private int validationTimeout = 5;

    /**
     * 空闲连接检查周期（毫秒），0表示不检查
     */
    private long idleCheckInterval = 60000;

    /**
     * 连接最大空闲时间（毫秒），超过此时间将被回收
     */
    private long maxIdleTime = 1800000;

    /**
     * 连接最大生命周期（毫秒），0表示无限制
     */
    private long maxLifetime = 0;

    /**
     * 是否开启连接泄漏检测
     */
    private boolean leakDetectionEnabled = false;

    /**
     * 连接泄漏检测阈值（毫秒）
     */
    private long leakDetectionThreshold = 60000;

    /**
     * 是否在归还时验证连接
     */
    private boolean testOnReturn = false;

    /**
     * 是否在借用时验证连接
     */
    private boolean testOnBorrow = true;

    /**
     * 是否在空闲时验证连接
     */
    private boolean testWhileIdle = true;

    /**
     * 是否记录SQL日志
     */
    private boolean logSql = false;

    /**
     * 连接池名称
     */
    private String poolName = "JQuickPool";

    public JQuickPoolConfig() {
    }

    /**
     * 构建默认配置
     */
    public static JQuickPoolConfig defaultConfig() {
        return new JQuickPoolConfig();
    }

    /**
     * 构建小规模连接池配置
     */
    public static JQuickPoolConfig smallPool() {
        return new JQuickPoolConfig()
                .setMinIdle(1)
                .setMaxActive(5)
                .setMaxIdle(3);
    }

    /**
     * 构建大规模连接池配置
     */
    public static JQuickPoolConfig largePool() {
        return new JQuickPoolConfig()
                .setMinIdle(10)
                .setMaxActive(100)
                .setMaxIdle(50)
                .setMaxWait(60000);
    }

    /**
     * 构建高并发连接池配置
     */
    public static JQuickPoolConfig highConcurrencyPool() {
        return new JQuickPoolConfig().setMinIdle(20).setMaxActive(200)
                .setMaxIdle(100).setMaxWait(15000)
                .setIdleCheckInterval(30000)
                .setTestOnBorrow(false)
                .setTestWhileIdle(true);
    }

    public int getMinIdle() {
        return minIdle;
    }

    public JQuickPoolConfig setMinIdle(int minIdle) {
        this.minIdle = Math.max(0, minIdle);
        return this;
    }

    public int getMaxActive() {
        return maxActive;
    }

    public JQuickPoolConfig setMaxActive(int maxActive) {
        this.maxActive = Math.max(1, maxActive);
        return this;
    }

    public int getMaxIdle() {
        return maxIdle;
    }

    public JQuickPoolConfig setMaxIdle(int maxIdle) {
        this.maxIdle = Math.max(0, maxIdle);
        return this;
    }

    public long getMaxWait() {
        return maxWait;
    }

    public JQuickPoolConfig setMaxWait(long maxWait) {
        this.maxWait = maxWait;
        return this;
    }

    public int getConnectionTimeout() {
        return connectionTimeout;
    }

    public JQuickPoolConfig setConnectionTimeout(int connectionTimeout) {
        this.connectionTimeout = connectionTimeout;
        return this;
    }

    public String getValidationQuery() {
        return validationQuery;
    }

    public JQuickPoolConfig setValidationQuery(String validationQuery) {
        this.validationQuery = validationQuery;
        return this;
    }

    public int getValidationTimeout() {
        return validationTimeout;
    }

    public JQuickPoolConfig setValidationTimeout(int validationTimeout) {
        this.validationTimeout = validationTimeout;
        return this;
    }

    public long getIdleCheckInterval() {
        return idleCheckInterval;
    }

    public JQuickPoolConfig setIdleCheckInterval(long idleCheckInterval) {
        this.idleCheckInterval = idleCheckInterval;
        return this;
    }

    public long getMaxIdleTime() {
        return maxIdleTime;
    }

    public JQuickPoolConfig setMaxIdleTime(long maxIdleTime) {
        this.maxIdleTime = maxIdleTime;
        return this;
    }

    public long getMaxLifetime() {
        return maxLifetime;
    }

    public JQuickPoolConfig setMaxLifetime(long maxLifetime) {
        this.maxLifetime = maxLifetime;
        return this;
    }

    public boolean isLeakDetectionEnabled() {
        return leakDetectionEnabled;
    }

    public JQuickPoolConfig setLeakDetectionEnabled(boolean leakDetectionEnabled) {
        this.leakDetectionEnabled = leakDetectionEnabled;
        return this;
    }

    public long getLeakDetectionThreshold() {
        return leakDetectionThreshold;
    }

    public JQuickPoolConfig setLeakDetectionThreshold(long leakDetectionThreshold) {
        this.leakDetectionThreshold = leakDetectionThreshold;
        return this;
    }

    public boolean isTestOnReturn() {
        return testOnReturn;
    }

    public JQuickPoolConfig setTestOnReturn(boolean testOnReturn) {
        this.testOnReturn = testOnReturn;
        return this;
    }

    public boolean isTestOnBorrow() {
        return testOnBorrow;
    }

    public JQuickPoolConfig setTestOnBorrow(boolean testOnBorrow) {
        this.testOnBorrow = testOnBorrow;
        return this;
    }

    public boolean isTestWhileIdle() {
        return testWhileIdle;
    }

    public JQuickPoolConfig setTestWhileIdle(boolean testWhileIdle) {
        this.testWhileIdle = testWhileIdle;
        return this;
    }

    public boolean isLogSql() {
        return logSql;
    }

    public JQuickPoolConfig setLogSql(boolean logSql) {
        this.logSql = logSql;
        return this;
    }

    public String getPoolName() {
        return poolName;
    }

    public JQuickPoolConfig setPoolName(String poolName) {
        this.poolName = poolName;
        return this;
    }

    @Override
    public String toString() {
        return "JQuickPoolConfig{" + "minIdle=" + minIdle + ", maxActive=" + maxActive + ", maxIdle=" + maxIdle + ", maxWait=" + maxWait + ", poolName='" + poolName + '\'' + '}';
    }
}
