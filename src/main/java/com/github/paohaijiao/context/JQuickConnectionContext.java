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
package com.github.paohaijiao.context;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * ThreadLocal 封装的 Connection 容器
 * 保证同一个线程内使用同一个连接，支持事务、自动释放
 */
public class JQuickConnectionContext {

    private static final ThreadLocal<Connection> CONNECTION_HOLDER = new ThreadLocal<>();


    /**
     * 获取当前线程的连接
     */
    public static Connection getConnection() {
        return CONNECTION_HOLDER.get();
    }

    /**
     * 绑定连接到当前线程
     */
    public static void setConnection(Connection connection) {
        if (connection != null) {
            CONNECTION_HOLDER.set(connection);
        }
    }

    /**
     * 移除当前线程的连接（必须调用，防止内存泄漏）
     */
    public static void remove() {
        CONNECTION_HOLDER.remove();
    }


    /**
     * 当前线程是否持有连接
     */
    public static boolean hasConnection() {
        return getConnection() != null;
    }

    /**
     * 开启事务（关闭自动提交）
     */
    public static void beginTransaction() throws SQLException {
        Connection conn = getConnection();
        if (conn != null) {
            conn.setAutoCommit(false);
        }
    }

    /**
     * 提交事务
     */
    public static void commit() throws SQLException {
        Connection conn = getConnection();
        if (conn != null) {
            conn.commit();
        }
    }

    /**
     * 回滚事务
     */
    public static void rollback() throws SQLException {
        Connection conn = getConnection();
        if (conn != null) {
            conn.rollback();
        }
    }

    /**
     * 关闭连接 + 移除 ThreadLocal
     */
    public static void close() {
        Connection conn = getConnection();
        try {
            if (conn != null && !conn.isClosed()) {
                conn.setAutoCommit(true);
                conn.close();
            }
        } catch (SQLException e) {
            e.printStackTrace();
        } finally {
            remove();
        }
    }

    /**
     * 安静关闭（不抛异常）
     */
    public static void closeQuietly() {
        try {
            close();
        } catch (Exception ignored) {
        }
    }
}
