package com.github.paohaijiao.factory;
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
import com.github.paohaijiao.connector.JQuickDataSourceConnector;
import com.github.paohaijiao.console.JConsole;
import com.github.paohaijiao.context.JQuickConnectionContext;
import com.github.paohaijiao.dialect.JQuickSQLDialect;
import com.github.paohaijiao.extra.JQuickForeignKeyConstraint;
import com.github.paohaijiao.extra.JQuickIndexDefinition;
import com.github.paohaijiao.extra.JQuickPrimaryKeyConstraint;
import com.github.paohaijiao.extra.JQuickUniqueConstraint;
import com.github.paohaijiao.manager.JQuickDatabaseTypeManager;
import com.github.paohaijiao.manager.JQuickPoolManager;
import com.github.paohaijiao.pool.JQuickConnectionPool;
import com.github.paohaijiao.statement.JQuickRow;
import com.github.paohaijiao.table.JQuickTableDefinition;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;

/**
 * JQuick SQL 执行工厂
 * 提供统一的 DDL/DML 操作入口，支持多数据库方言自动适配
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/17
 */
public class JQuickSQLFactory implements AutoCloseable {

    private static final JConsole console = new JConsole();

    private final JQuickDataSourceConnector connector;

    private final JQuickSQLDialect dialect;

    private final JQuickTableDefinition defaultTableDefinition;

    private boolean autoCommit = true;

    private boolean ownsConnection = false;

    private JQuickConnectionPool pool;

    /**
     * 使用方言创建工厂（从 JQuickConnectionContext 获取连接）
     *
     * @param dialect SQL方言
     */
    public JQuickSQLFactory(JQuickSQLDialect dialect) {
        this(dialect, null, null);
    }

    /**
     * 使用方言和默认表定义创建工厂（从 JQuickConnectionContext 获取连接）
     *
     * @param dialect                SQL方言
     * @param defaultTableDefinition 默认表定义
     */
    public JQuickSQLFactory(JQuickSQLDialect dialect, JQuickTableDefinition defaultTableDefinition) {
        this(dialect, defaultTableDefinition, null);
    }

    /**
     * 使用方言、默认表定义和连接配置创建工厂
     * 如果 JQuickConnectionContext 中没有连接，则使用 connector 创建新连接并绑定
     *
     * @param dialect                SQL方言
     * @param defaultTableDefinition 默认表定义
     * @param connector              数据源连接配置（可选，当 ThreadLocal 中无连接时使用）
     */
    public JQuickSQLFactory(JQuickSQLDialect dialect, JQuickTableDefinition defaultTableDefinition, JQuickDataSourceConnector connector) {
        this.dialect = dialect;
        this.defaultTableDefinition = defaultTableDefinition;
        this.connector = connector;
        console.info("JQuickSQLFactory initialized with dialect: " + dialect.getClass().getSimpleName());
    }

    /**
     * 使用连接配置创建工厂（自动检测方言，并从 JQuickConnectionContext 获取或创建连接）
     *
     * @param connector 数据源连接配置
     */
    public JQuickSQLFactory(JQuickDataSourceConnector connector) {
        this(connector, null);
    }

    /**
     * 使用连接配置和默认表定义创建工厂
     *
     * @param connector              数据源连接配置
     * @param defaultTableDefinition 默认表定义
     */
    public JQuickSQLFactory(JQuickDataSourceConnector connector, JQuickTableDefinition defaultTableDefinition) {
        this.connector = connector;
        this.defaultTableDefinition = defaultTableDefinition;
        JQuickDatabaseTypeManager manager = JQuickDatabaseTypeManager.getInstance();
        if (connector.getType() != null && !connector.getType().isEmpty()) {
            this.dialect = manager.getDialect(connector.getType());
            if (this.dialect == null) {
                throw new IllegalArgumentException("Cannot detect database type from databaseType: " + connector.getUrl());
            }
        } else if (connector.getUrl() != null && !connector.getUrl().isEmpty()) {
            this.dialect = manager.getDialectByJdbcUrl(connector.getUrl());
            if (this.dialect == null) {
                throw new IllegalArgumentException("Cannot detect database type from URL: " + connector.getUrl());
            }
        } else {
            throw new IllegalArgumentException("URL must be provided for auto-detection");
        }
        console.info("JQuickSQLFactory initialized with dialect: " + dialect.getClass().getSimpleName());
    }

    /**
     * 使用指定的数据库类型和连接配置创建工厂
     *
     * @param connector              数据源连接配置
     * @param databaseType           数据库类型
     * @param defaultTableDefinition 默认表定义
     */
    public JQuickSQLFactory(JQuickDataSourceConnector connector, String databaseType, JQuickTableDefinition defaultTableDefinition) {
        this.connector = connector;
        this.defaultTableDefinition = defaultTableDefinition;
        JQuickDatabaseTypeManager manager = JQuickDatabaseTypeManager.getInstance();
        this.dialect = manager.getDialect(databaseType);
        if (this.dialect == null) {
            throw new IllegalArgumentException("Unsupported database type: " + databaseType);
        }
        console.info("JQuickSQLFactory initialized with dialect: " + dialect.getClass().getSimpleName());
    }

    /**
     * 获取数据库连接（从 JQuickConnectionContext 获取，若无则创建并绑定）
     */
    public Connection getConnection() throws SQLException {
        Connection conn = JQuickConnectionContext.getConnection();
        if (conn == null) {
            if (connector != null) {// ThreadLocal 中没有连接，尝试创建新连接
                conn = dialect.getConnection(connector);
                if (conn == null) {
                    throw new SQLException("Failed to create database connection");
                }
                conn.setAutoCommit(autoCommit);
                JQuickConnectionContext.setConnection(conn);
                ownsConnection = true;
                console.info("Created and bound new connection to ThreadLocal");
            } else {
                throw new SQLException("No connection available in ThreadLocal and no connector provided");
            }
        }

        return conn;
    }

    /**
     * 检查当前线程是否有连接
     */
    public boolean hasConnection() {
        return JQuickConnectionContext.hasConnection();
    }

    /**
     * 设置自动提交模式
     */
    public void setAutoCommit(boolean autoCommit) throws SQLException {
        this.autoCommit = autoCommit;
        Connection conn = JQuickConnectionContext.getConnection();
        if (conn != null && !conn.isClosed()) {
            conn.setAutoCommit(autoCommit);
        }
    }

    /**
     * 开启事务（使用 JQuickConnectionContext）
     */
    public void beginTransaction() throws SQLException {
        JQuickConnectionContext.beginTransaction();
        this.autoCommit = false;
    }

    /**
     * 提交事务（使用 JQuickConnectionContext）
     */
    public void commit() throws SQLException {
        JQuickConnectionContext.commit();
        this.autoCommit = true;
    }

    /**
     * 回滚事务（使用 JQuickConnectionContext）
     */
    public void rollback() throws SQLException {
        JQuickConnectionContext.rollback();
        this.autoCommit = true;
    }

    /**
     * 执行原生SQL
     */
    public boolean execute(String sql) throws SQLException {
        console.info("Executing SQL: " + sql);
        try (Statement stmt = getConnection().createStatement()) {
            return stmt.execute(sql);
        }
    }

    /**
     * 执行更新（INSERT/UPDATE/DELETE/DDL）
     */
    public int executeUpdate(String sql) throws SQLException {
        console.info("Executing update: " + sql);
        try (Statement stmt = getConnection().createStatement()) {
            return stmt.executeUpdate(sql);
        }
    }

    /**
     * 执行查询
     */
    public ResultSet executeQuery(String sql) throws SQLException {
        console.info("Executing query: " + sql);
        Statement stmt = getConnection().createStatement();
        return stmt.executeQuery(sql);
    }

    /**
     * 执行查询并自动转换为 JQuickRow 列表
     */
    public List<JQuickRow> executeQueryForRows(String sql) throws SQLException {
        List<JQuickRow> results = new ArrayList<>();
        try (Statement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(sql)) {
            ResultSetMetaData metaData = rs.getMetaData();
            int columnCount = metaData.getColumnCount();
            while (rs.next()) {
                JQuickRow row = new JQuickRow();
                for (int i = 1; i <= columnCount; i++) {
                    row.put(metaData.getColumnName(i), rs.getObject(i));
                }
                results.add(row);
            }
        }
        return results;
    }

    /**
     * 创建表
     */
    public boolean createTable(JQuickTableDefinition tableDefinition) throws SQLException {
        String sql = dialect.buildCreateTable(tableDefinition);
        return executeUpdate(sql) >= 0;
    }

    /**
     * 创建表（使用默认表定义）
     */
    public boolean createTable() throws SQLException {
        if (defaultTableDefinition == null) {
            throw new IllegalStateException("No default table definition set");
        }
        return createTable(defaultTableDefinition);
    }

    /**
     * 创建表（如果不存在）
     */
    public boolean createTableIfNotExists(JQuickTableDefinition tableDefinition) throws SQLException {
        if (tableDefinition.getExtensions() == null) {
            tableDefinition.setExtensions(new java.util.HashMap<>());
        }
        tableDefinition.getExtensions().put("ifNotExists", true);
        return createTable(tableDefinition);
    }

    /**
     * 修改列
     */
    public boolean modifyColumn(String tableName, JQuickColumnDefinition column) throws SQLException {
        String sql = dialect.buildModifyColumn(defaultTableDefinition, tableName, column);
        return executeUpdate(sql) >= 0;
    }

    /**
     * 修改列（使用默认表定义的表名）
     */
    public boolean modifyColumn(JQuickColumnDefinition column) throws SQLException {
        if (defaultTableDefinition == null) {
            throw new IllegalStateException("No default table definition set");
        }
        return modifyColumn(defaultTableDefinition.getTableName(), column);
    }

    /**
     * 重命名列
     */
    public boolean changeColumn(String tableName, String oldName, JQuickColumnDefinition newColumn) throws SQLException {
        String sql = dialect.buildChangeColumn(defaultTableDefinition, tableName, oldName, newColumn);
        return executeUpdate(sql) >= 0;
    }

    /**
     * 创建索引
     */
    public boolean createIndex(String tableName, JQuickIndexDefinition index) throws SQLException {
        String sql = dialect.buildIndex(defaultTableDefinition, index);
        sql = sql.replace("${tableName}", quoteIdentifier(tableName));
        return executeUpdate(sql) >= 0;
    }

    /**
     * 创建索引（使用默认表定义的表名）
     */
    public boolean createIndex(JQuickIndexDefinition index) throws SQLException {
        if (defaultTableDefinition == null) {
            throw new IllegalStateException("No default table definition set");
        }
        return createIndex(defaultTableDefinition.getTableName(), index);
    }

    /**
     * 添加主键
     */
    public boolean addPrimaryKey(String tableName, JQuickPrimaryKeyConstraint pk) throws SQLException {
        String sql = dialect.buildPrimaryKey(defaultTableDefinition, pk);
        sql = "ALTER TABLE " + quoteIdentifier(tableName) + " ADD " + sql;
        return executeUpdate(sql) >= 0;
    }

    /**
     * 添加唯一约束
     */
    public boolean addUniqueConstraint(String tableName, JQuickUniqueConstraint uc) throws SQLException {
        String sql = dialect.buildUniqueConstraint(defaultTableDefinition, uc);
        sql = "ALTER TABLE " + quoteIdentifier(tableName) + " ADD " + sql;
        return executeUpdate(sql) >= 0;
    }

    /**
     * 添加外键约束
     */
    public boolean addForeignKey(String tableName, JQuickForeignKeyConstraint fk) throws SQLException {
        String sql = dialect.buildForeignKey(defaultTableDefinition, fk);
        sql = "ALTER TABLE " + quoteIdentifier(tableName) + " ADD " + sql;
        return executeUpdate(sql) >= 0;
    }

    /**
     * 获取建表语句
     */
    public String getShowCreateTable(String tableName) throws SQLException {
        return dialect.buildShowCreateTable(defaultTableDefinition, tableName);
    }

    /**
     * 获取表结构描述
     */
    public String getDescribeTable(String tableName) throws SQLException {
        return dialect.buildDescribeTable(defaultTableDefinition, tableName);
    }

    /**
     * 插入数据
     */
    public int insert(JQuickTableDefinition tableDefinition, JQuickRow row) throws SQLException {
        String sql = dialect.buildInsert(tableDefinition, row);
        return executeUpdate(sql);
    }

    /**
     * 插入数据（使用默认表定义）
     */
    public int insert(JQuickRow row) throws SQLException {
        if (defaultTableDefinition == null) {
            throw new IllegalStateException("No default table definition set");
        }
        return insert(defaultTableDefinition, row);
    }

    /**
     * 批量插入（使用 Statement 批量执行）
     */
    public int[] insertBatch(JQuickTableDefinition tableDefinition, List<JQuickRow> rows) throws SQLException {
        if (rows == null || rows.isEmpty()) {
            return new int[0];
        }
        try (Statement stmt = getConnection().createStatement()) {
            for (JQuickRow row : rows) {
                String sql = dialect.buildInsert(tableDefinition, row);
                stmt.addBatch(sql);
            }
            return stmt.executeBatch();
        }
    }

    /**
     * 批量插入（使用默认表定义）
     */
    public int[] insertBatch(List<JQuickRow> rows) throws SQLException {
        if (defaultTableDefinition == null) {
            throw new IllegalStateException("No default table definition set");
        }
        return insertBatch(defaultTableDefinition, rows);
    }

    /**
     * 使用 PreparedStatement 批量插入（性能更好）
     */
    public int[] insertBatchPrepared(JQuickTableDefinition tableDefinition, List<JQuickRow> rows, List<String> columns) throws SQLException {
        if (rows == null || rows.isEmpty() || columns == null || columns.isEmpty()) {
            return new int[0];
        }
        StringBuilder sql = new StringBuilder();
        sql.append("INSERT INTO ").append(quoteIdentifier(tableDefinition.getTableName()));
        sql.append(" (").append(columns.stream().map(this::quoteIdentifier).reduce((a, b) -> a + ", " + b).orElse(""));
        sql.append(") VALUES (");
        sql.append(columns.stream().map(c -> "?").reduce((a, b) -> a + ", " + b).orElse(""));
        sql.append(")");
        try (PreparedStatement pstmt = getConnection().prepareStatement(sql.toString())) {
            for (JQuickRow row : rows) {
                int idx = 1;
                for (String col : columns) {
                    pstmt.setObject(idx++, row.get(col));
                }
                pstmt.addBatch();
            }
            return pstmt.executeBatch();
        }
    }

    /**
     * 更新数据
     */
    public int update(JQuickTableDefinition tableDefinition, JQuickRow row, String whereClause) throws SQLException {
        String sql = dialect.buildUpdate(tableDefinition, row, whereClause);
        return executeUpdate(sql);
    }

    /**
     * 更新数据（使用默认表定义）
     */
    public int update(JQuickRow row, String whereClause) throws SQLException {
        if (defaultTableDefinition == null) {
            throw new IllegalStateException("No default table definition set");
        }
        return update(defaultTableDefinition, row, whereClause);
    }

    /**
     * 删除数据
     */
    public int delete(JQuickTableDefinition tableDefinition, String whereClause) throws SQLException {
        String sql = dialect.buildDelete(tableDefinition, whereClause);
        return executeUpdate(sql);
    }

    /**
     * 删除数据（使用默认表定义）
     */
    public int delete(String whereClause) throws SQLException {
        if (defaultTableDefinition == null) {
            throw new IllegalStateException("No default table definition set");
        }
        return delete(defaultTableDefinition, whereClause);
    }

    /**
     * 删除所有数据
     */
    public int deleteAll() throws SQLException {
        return delete((String) null);
    }

    /**
     * 查询数据
     */
    public List<JQuickRow> select(JQuickTableDefinition tableDefinition, List<String> columns, String whereClause) throws SQLException {
        String sql = dialect.buildSelect(tableDefinition, columns, whereClause);
        return executeQueryForRows(sql);
    }

    /**
     * 查询数据（使用默认表定义）
     */
    public List<JQuickRow> select(List<String> columns, String whereClause) throws SQLException {
        if (defaultTableDefinition == null) {
            throw new IllegalStateException("No default table definition set");
        }
        return select(defaultTableDefinition, columns, whereClause);
    }

    /**
     * 查询所有数据
     */
    public List<JQuickRow> selectAll(String whereClause) throws SQLException {
        return select(null, whereClause);
    }

    /**
     * 查询所有数据（无条件）
     */
    public List<JQuickRow> selectAll() throws SQLException {
        return selectAll(null);
    }

    /**
     * 查询单条数据
     */
    public JQuickRow selectOne(JQuickTableDefinition tableDefinition, String whereClause) throws SQLException {
        List<JQuickRow> results = select(tableDefinition, null, whereClause);
        return results.isEmpty() ? null : results.get(0);
    }

    /**
     * 查询单条数据（使用默认表定义）
     */
    public JQuickRow selectOne(String whereClause) throws SQLException {
        return selectOne(defaultTableDefinition, whereClause);
    }

    /**
     * 根据ID查询
     */
    public JQuickRow selectById(String idColumn, Object idValue) throws SQLException {
        if (defaultTableDefinition == null) {
            throw new IllegalStateException("No default table definition set");
        }
        String whereClause = quoteIdentifier(idColumn) + " = " + formatValue(idValue);
        return selectOne(whereClause);
    }

    /**
     * 统计记录数
     */
    public long count(JQuickTableDefinition tableDefinition, String whereClause) throws SQLException {
        StringBuilder sql = new StringBuilder();
        sql.append("SELECT COUNT(*) FROM ").append(quoteIdentifier(tableDefinition.getTableName()));
        if (whereClause != null && !whereClause.isEmpty()) {
            sql.append(" WHERE ").append(whereClause);
        }
        try (Statement stmt = getConnection().createStatement();
             ResultSet rs = stmt.executeQuery(sql.toString())) {
            if (rs.next()) {
                return rs.getLong(1);
            }
        }
        return 0;
    }

    /**
     * 统计记录数（使用默认表定义）
     */
    public long count(String whereClause) throws SQLException {
        if (defaultTableDefinition == null) {
            throw new IllegalStateException("No default table definition set");
        }
        return count(defaultTableDefinition, whereClause);
    }

    /**
     * 统计总记录数
     */
    public long countAll() throws SQLException {
        return count((String) null);
    }

    /**
     * 检查记录是否存在
     */
    public boolean exists(String whereClause) throws SQLException {
        return count(whereClause) > 0;
    }

    /**
     * 批量执行SQL
     */
    public int[] executeBatch(List<String> sqls) throws SQLException {
        try (Statement stmt = getConnection().createStatement()) {
            for (String sql : sqls) {
                stmt.addBatch(sql);
            }
            return stmt.executeBatch();
        }
    }

    /**
     * 批量执行更新（相同SQL，不同参数）
     */
    public int[] executeBatch(String sql, List<Object[]> paramsList) throws SQLException {
        try (PreparedStatement pstmt = getConnection().prepareStatement(sql)) {
            for (Object[] params : paramsList) {
                for (int i = 0; i < params.length; i++) {
                    pstmt.setObject(i + 1, params[i]);
                }
                pstmt.addBatch();
            }
            return pstmt.executeBatch();
        }
    }

    /**
     * 引用标识符
     */
    private String quoteIdentifier(String identifier) {
        if (identifier == null || identifier.isEmpty()) {
            return identifier;
        }
        if (dialect != null && defaultTableDefinition != null) {
            String temp = dialect.buildSelect(defaultTableDefinition, null, null);
            if (temp.contains("`")) {
                return "`" + identifier + "`";
            } else if (temp.contains("\"")) {
                return "\"" + identifier + "\"";
            } else if (temp.contains("[")) {
                return "[" + identifier + "]";
            }
        }
        return identifier;
    }

    /**
     * 格式化值
     */
    private String formatValue(Object value) {
        if (value == null) {
            return "NULL";
        }
        if (value instanceof String) {
            return "'" + escapeString((String) value) + "'";
        }
        if (value instanceof Number || value instanceof Boolean) {
            return value.toString();
        }
        if (value instanceof Date) {
            return "'" + value.toString() + "'";
        }
        if (value instanceof Timestamp) {
            return "'" + value.toString() + "'";
        }
        return "'" + escapeString(value.toString()) + "'";
    }

    /**
     * 转义字符串
     */
    private String escapeString(String str) {
        if (str == null) return "";
        return str.replace("\\", "\\\\")
                .replace("'", "\\'")
                .replace("\"", "\\\"");
    }

    /**
     * 获取方言实例
     */
    public JQuickSQLDialect getDialect() {
        return dialect;
    }

    /**
     * 获取默认表定义
     */
    public JQuickTableDefinition getDefaultTableDefinition() {
        return defaultTableDefinition;
    }

    /**
     * 设置默认表定义
     */
    public JQuickSQLFactory withDefaultTable(JQuickTableDefinition tableDefinition) {
        return new JQuickSQLFactory(dialect, tableDefinition, connector);
    }

    /**
     * 检查工厂是否拥有连接（即由工厂创建并绑定到 ThreadLocal）
     */
    public boolean ownsConnection() {
        return ownsConnection;
    }

    /**
     * 关闭连接（仅当工厂拥有连接时）
     * 注意：不会清除 ThreadLocal 中的连接，如需清除请调用 JQuickConnectionContext.remove()
     */
    @Override
    public void close() {
        if (ownsConnection) {
            JQuickConnectionContext.closeQuietly();
            ownsConnection = false;
            console.info("Connection closed by factory");
        }
    }

    /**
     * 关闭连接并清除 ThreadLocal
     */
    public void closeAndRemove() {
        if (ownsConnection) {
            JQuickConnectionContext.close();
            ownsConnection = false;
            console.info("Connection closed and ThreadLocal removed by factory");
        } else {
            // 如果工厂不拥有连接，只移除 ThreadLocal 中的连接（不关闭）
            JQuickConnectionContext.remove();
            console.info("ThreadLocal connection removed by factory");
        }
    }

    /**
     * 使用连接池获取连接
     */
    public Connection getPooledConnection() throws SQLException {
        if (pool == null) {
            if (connector != null && dialect != null) {
                pool = JQuickPoolManager.getInstance().createPool(connector, dialect, "factory_" + System.identityHashCode(this));
            } else {
                throw new SQLException("Cannot create pool: connector or dialect missing");
            }
        }
        return pool.getConnection();
    }

    /**
     * 使用连接池执行更新
     */
    public int executeUpdateWithPool(String sql) throws SQLException {
        console.info("Executing update with pool: " + sql);
        try (Connection conn = getPooledConnection(); Statement stmt = conn.createStatement()) {
            return stmt.executeUpdate(sql);
        }
    }

    /**
     * 使用连接池执行查询
     */
    public ResultSet executeQueryWithPool(String sql) throws SQLException {
        console.info("Executing query with pool: " + sql);
        Connection conn = getPooledConnection();
        Statement stmt = conn.createStatement();
        return stmt.executeQuery(sql);
    }

    /**
     * 设置连接池
     */
    public JQuickSQLFactory withPool(JQuickConnectionPool pool) {
        this.pool = pool;
        return this;
    }

    /**
     * 获取连接池
     */
    public JQuickConnectionPool getPool() {
        return pool;
    }

    /**
     * 关闭连接池
     */
    public void closePool() {
        if (pool != null) {
            pool.close();
            pool = null;
        }

    }
}