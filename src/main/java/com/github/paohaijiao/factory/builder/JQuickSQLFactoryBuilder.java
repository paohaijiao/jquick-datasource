package com.github.paohaijiao.factory.builder;

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
import com.github.paohaijiao.connector.JQuickDataSourceConnector;
import com.github.paohaijiao.context.JQuickConnectionContext;
import com.github.paohaijiao.dialect.JQuickSQLDialect;
import com.github.paohaijiao.factory.JQuickSQLFactory;
import com.github.paohaijiao.manager.JQuickDatabaseTypeManager;
import com.github.paohaijiao.table.JQuickTableDefinition;
import java.sql.Connection;
import java.sql.SQLException;

/**
 * JQuickSQLFactory 构建器
 * 提供链式调用的工厂创建方式
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/17
 */
public class JQuickSQLFactoryBuilder {

    private JQuickDataSourceConnector connector;

    private JQuickSQLDialect dialect;

    private String databaseType;

    private JQuickTableDefinition defaultTableDefinition;

    private boolean autoCommit = true;

    private boolean bindConnection = false;

    /**
     * 设置数据源连接配置
     */
    public JQuickSQLFactoryBuilder connector(JQuickDataSourceConnector connector) {
        this.connector = connector;
        return this;
    }

    /**
     * 设置 JDBC URL
     */
    public JQuickSQLFactoryBuilder url(String url) {
        if (connector == null) {
            connector = new JQuickDataSourceConnector();
        }
        connector.setUrl(url);
        return this;
    }

    /**
     * 设置用户名
     */
    public JQuickSQLFactoryBuilder username(String username) {
        if (connector == null) {
            connector = new JQuickDataSourceConnector();
        }
        connector.setUsername(username);
        return this;
    }

    /**
     * 设置密码
     */
    public JQuickSQLFactoryBuilder password(String password) {
        if (connector == null) {
            connector = new JQuickDataSourceConnector();
        }
        connector.setPassword(password);
        return this;
    }

    /**
     * 设置数据库名/Schema
     */
    public JQuickSQLFactoryBuilder schema(String schema) {
        if (connector == null) {
            connector = new JQuickDataSourceConnector();
        }
        connector.setSchema(schema);
        return this;
    }

    /**
     * 设置主机
     */
    public JQuickSQLFactoryBuilder host(String host) {
        if (connector == null) {
            connector = new JQuickDataSourceConnector();
        }
        connector.setHost(host);
        return this;
    }

    /**
     * 设置端口
     */
    public JQuickSQLFactoryBuilder port(String port) {
        if (connector == null) {
            connector = new JQuickDataSourceConnector();
        }
        connector.setPort(port);
        return this;
    }

    /**
     * 直接设置方言
     */
    public JQuickSQLFactoryBuilder dialect(JQuickSQLDialect dialect) {
        this.dialect = dialect;
        return this;
    }

    /**
     * 设置数据库类型（用于自动获取方言）
     */
    public JQuickSQLFactoryBuilder databaseType(String databaseType) {
        this.databaseType = databaseType;
        return this;
    }

    /**
     * 设置默认表定义
     */
    public JQuickSQLFactoryBuilder defaultTable(JQuickTableDefinition tableDefinition) {
        this.defaultTableDefinition = tableDefinition;
        return this;
    }

    /**
     * 设置自动提交模式
     */
    public JQuickSQLFactoryBuilder autoCommit(boolean autoCommit) {
        this.autoCommit = autoCommit;
        return this;
    }

    /**
     * 是否自动绑定连接到 ThreadLocal
     */
    public JQuickSQLFactoryBuilder bindConnection(boolean bindConnection) {
        this.bindConnection = bindConnection;
        return this;
    }

    /**
     * 构建工厂实例
     */
    public JQuickSQLFactory build() throws SQLException {
        JQuickSQLFactory factory;
        JQuickSQLDialect finalDialect = dialect;
        if (finalDialect == null) {
            JQuickDatabaseTypeManager manager = JQuickDatabaseTypeManager.getInstance();
            if (databaseType != null && !databaseType.isEmpty()) {
                finalDialect = manager.getDialect(databaseType);
            } else if (connector != null && connector.getUrl() != null) {
                finalDialect = manager.getDialectByJdbcUrl(connector.getUrl());
            }
        }
        if (finalDialect == null) {
            throw new IllegalArgumentException("Cannot determine SQL dialect. Please provide dialect, databaseType, or valid URL.");
        }
        if (connector != null) {
            factory = new JQuickSQLFactory(connector, databaseType, defaultTableDefinition);
        } else {
            factory = new JQuickSQLFactory(finalDialect, defaultTableDefinition, null);
        }
        factory.setAutoCommit(autoCommit);
        if (bindConnection && connector != null && JQuickConnectionContext.getConnection() == null) {
            Connection conn = finalDialect.getConnection(connector);
            if (conn != null) {
                conn.setAutoCommit(autoCommit);
                JQuickConnectionContext.setConnection(conn);
            }
        }
        return factory;
    }

    /**
     * 构建并自动绑定连接
     */
    public JQuickSQLFactory buildAndBind() throws SQLException {
        bindConnection = true;
        return build();
    }
}