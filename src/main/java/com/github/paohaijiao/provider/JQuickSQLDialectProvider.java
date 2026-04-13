package com.github.paohaijiao.provider;

import com.github.paohaijiao.dialect.JQuickSQLDialect;

public interface JQuickSQLDialectProvider {

    /**
     * 获取数据库类型名称
     * @return 数据库类型，如 "mysql", "postgresql", "oracle" 等
     */
    String getDatabaseType();

    /**
     * 创建方言实例
     * @return SQLDialect 实例
     */
    JQuickSQLDialect createDialect();

    /**
     * 获取优先级（数值越小优先级越高）
     * @return 优先级
     */
    default int getPriority() {
        return 100;
    }

    /**
     * 是否支持自动检测
     * @param jdbcUrl JDBC URL
     * @return 是否支持
     */
    default boolean supports(String jdbcUrl) {
        return false;
    }
}
