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
import com.github.paohaijiao.context.JQuickConnectionContext;
import com.github.paohaijiao.dataType.JQuickDataType;
import com.github.paohaijiao.dataType.enums.JQuickDataTypeFamily;
import com.github.paohaijiao.dialect.JQuickSQLDialect;
import com.github.paohaijiao.dialect.impl.JQuickAccessDialect;
import com.github.paohaijiao.dialect.impl.JQuickBigQueryDialect;
import com.github.paohaijiao.dialect.impl.JQuickMySQLDialect;
import com.github.paohaijiao.extra.JQuickForeignKeyConstraint;
import com.github.paohaijiao.extra.JQuickIndexDefinition;
import com.github.paohaijiao.extra.JQuickPrimaryKeyConstraint;
import com.github.paohaijiao.extra.JQuickUniqueConstraint;
import com.github.paohaijiao.factory.builder.JQuickSQLFactoryBuilder;
import com.github.paohaijiao.manager.JQuickDatabaseTypeManager;
import com.github.paohaijiao.statement.JQuickRow;
import com.github.paohaijiao.table.JQuickTableDefinition;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.*;

import static org.junit.Assert.*;

/**
 * JQuickSQLFactory 测试用例
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/17
 */
public class JQuickSqlFactoryTest {

    private JQuickTableDefinition testTableDefinition;
    private JQuickDataSourceConnector testConnector;

    @Before
    public void setUp() {
        testTableDefinition = createTestTableDefinition();
        testConnector = new JQuickDataSourceConnector();
        testConnector.setDriverClass("org.h2.Driver");
        testConnector.setUrl("jdbc:h2:mem:testdb;DB_CLOSE_DELAY=-1");
        testConnector.setUsername("sa");
        testConnector.setPassword("");
        testConnector.setSchema("PUBLIC");
    }

    @After
    public void tearDown() {
        JQuickConnectionContext.closeQuietly();
    }

    /**
     * 创建测试表定义
     */
    private JQuickTableDefinition createTestTableDefinition() {
        JQuickTableDefinition tableDefinition = new JQuickTableDefinition();
        tableDefinition.setTableName("test_user");
        tableDefinition.setComment("用户信息测试表");
        tableDefinition.setQuoteEnabled(true);
        JQuickColumnDefinition idColumn = createColumn("id", JQuickDataTypeFamily.LONG);
        idColumn.setAutoIncrement(true);
        idColumn.setNullable(false);
        idColumn.setComment("主键ID");
        JQuickColumnDefinition nameColumn = createColumn("name", JQuickDataTypeFamily.STRING);
        nameColumn.setLength(100);
        nameColumn.setNullable(false);
        nameColumn.setComment("用户姓名");
        JQuickColumnDefinition ageColumn = createColumn("age", JQuickDataTypeFamily.INTEGER);
        ageColumn.setComment("年龄");
        // 邮箱列
        JQuickColumnDefinition emailColumn = createColumn("email", JQuickDataTypeFamily.STRING);
        emailColumn.setLength(200);
        emailColumn.setNullable(false);
        emailColumn.setDefaultValue("unknown@example.com");
        // 创建时间列
        JQuickColumnDefinition createdTimeColumn = createColumn("created_time", JQuickDataTypeFamily.TIMESTAMP);
        createdTimeColumn.setDefaultValue("CURRENT_TIMESTAMP");
        createdTimeColumn.setComment("创建时间");
        tableDefinition.setColumns(Arrays.asList(idColumn, nameColumn, ageColumn, emailColumn, createdTimeColumn));
        // 添加主键
        JQuickPrimaryKeyConstraint pk = new JQuickPrimaryKeyConstraint();
        pk.setConstraintName("pk_test_user");
        pk.setColumns(Collections.singletonList("id"));
        tableDefinition.setPrimaryKeys(Collections.singletonList(pk));
        // 添加唯一约束
        JQuickUniqueConstraint uniqueConstraint = new JQuickUniqueConstraint();
        uniqueConstraint.setConstraintName("uk_test_user_email");
        uniqueConstraint.setColumns(Collections.singletonList("email"));
        tableDefinition.setUniqueConstraints(Collections.singletonList(uniqueConstraint));
        return tableDefinition;
    }

    /**
     * 创建列定义
     */
    private JQuickColumnDefinition createColumn(String name, JQuickDataTypeFamily family) {
        JQuickColumnDefinition column = new JQuickColumnDefinition();
        column.setColumnName(name);
        JQuickDataType dataType = new JQuickDataType();
        dataType.setFamily(family);
        dataType.setParameters(new HashMap<>());
        column.setDataType(dataType);
        return column;
    }

    /**
     * 创建测试行数据
     */
    private JQuickRow createTestRow(String name, int age, String email) {
        JQuickRow row = new JQuickRow();
        row.put("name", name);
        row.put("age", age);
        row.put("email", email);
        return row;
    }

    @Test
    public void testConstructorWithConnector() {
        try (JQuickSQLFactory factory = new JQuickSQLFactory(testConnector)) {
            assertNotNull(factory);
            assertNotNull(factory.getDialect());
        } catch (Exception e) {
            fail("Failed to create factory: " + e.getMessage());
        }
    }

    @Test
    public void testConstructorWithConnectorAndTable() {
        try (JQuickSQLFactory factory = new JQuickSQLFactory(testConnector, testTableDefinition)) {
            assertNotNull(factory);
            assertEquals(testTableDefinition, factory.getDefaultTableDefinition());
        } catch (Exception e) {
            fail("Failed to create factory: " + e.getMessage());
        }
    }

    @Test
    public void testConstructorWithDialect() {
        JQuickSQLDialect dialect = new JQuickMySQLDialect();
        try (JQuickSQLFactory factory = new JQuickSQLFactory(dialect)) {
            assertNotNull(factory);
            assertEquals(dialect, factory.getDialect());
        }
    }

    @Test
    public void testConstructorWithDialectAndTable() {
        JQuickSQLDialect dialect = new JQuickMySQLDialect();
        try (JQuickSQLFactory factory = new JQuickSQLFactory(dialect, testTableDefinition)) {
            assertNotNull(factory);
            assertEquals(dialect, factory.getDialect());
            assertEquals(testTableDefinition, factory.getDefaultTableDefinition());
        }
    }

    @Test
    public void testConstructorWithConnectorAndDatabaseType() {
        try (JQuickSQLFactory factory = new JQuickSQLFactory(testConnector, "mysql", testTableDefinition)) {
            assertNotNull(factory);
            assertNotNull(factory.getDialect());
        } catch (Exception e) {
            fail("Failed to create factory: " + e.getMessage());
        }
    }

    @Test
    public void testBuilder() throws SQLException {
        JQuickSQLFactory factory = new JQuickSQLFactoryBuilder()
                .url("jdbc:h2:mem:testdb")
                .username("sa")
                .password("")
                .databaseType("mysql")
                .defaultTable(testTableDefinition)
                .autoCommit(true)
                .build();
        assertNotNull(factory);
        assertNotNull(factory.getDialect());
        assertEquals(testTableDefinition, factory.getDefaultTableDefinition());
        factory.close();
    }

    @Test
    public void testBuilderWithBindConnection() throws SQLException {
        JQuickSQLFactory factory = new JQuickSQLFactoryBuilder()
                .url("jdbc:h2:mem:testdb")
                .username("sa")
                .password("")
                .defaultTable(testTableDefinition)
                .bindConnection(true)
                .build();
        assertNotNull(factory);
        assertTrue(factory.hasConnection());
        factory.closeAndRemove();
    }

    @Test
    public void testGetConnection() throws SQLException {
        try (JQuickSQLFactory factory = new JQuickSQLFactory(testConnector)) {
            Connection conn = factory.getConnection();
            assertNotNull(conn);
            assertFalse(conn.isClosed());
        }
    }

    @Test
    public void testHasConnection() throws SQLException {
        try (JQuickSQLFactory factory = new JQuickSQLFactory(testConnector)) {
            assertFalse(factory.hasConnection());
            factory.getConnection();
            assertTrue(factory.hasConnection());
        }
    }

    @Test
    public void testAutoCommit() throws SQLException {
        try (JQuickSQLFactory factory = new JQuickSQLFactory(testConnector)) {
            factory.setAutoCommit(false);
            Connection conn = factory.getConnection();
            assertFalse(conn.getAutoCommit());
            factory.setAutoCommit(true);
            conn = factory.getConnection();
            assertTrue(conn.getAutoCommit());
        }
    }

    @Test
    public void testTransaction() throws SQLException {
        try (JQuickSQLFactory factory = new JQuickSQLFactory(testConnector)) {
            // 创建表
            factory.createTable(testTableDefinition);
            // 开启事务
            factory.beginTransaction();
            Connection conn = factory.getConnection();
            assertFalse(conn.getAutoCommit());
            // 插入数据
            JQuickRow row = createTestRow("张三", 25, "zhangsan@test.com");
            int result = factory.insert(row);
            assertEquals(1, result);
            // 提交事务
            factory.commit();
            assertTrue(conn.getAutoCommit());
            // 验证数据存在
            long count = factory.countAll();
            assertEquals(1, count);
        }
    }

    @Test
    public void testRollback() throws SQLException {
        try (JQuickSQLFactory factory = new JQuickSQLFactory(testConnector)) {
            factory.createTable(testTableDefinition);
            // 开启事务
            factory.beginTransaction();
            // 插入数据
            JQuickRow row = createTestRow("李四", 30, "lisi@test.com");
            factory.insert(row);
            // 回滚事务
            factory.rollback();
            // 验证数据不存在
            long count = factory.countAll();
            assertEquals(0, count);
        }
    }
    @Test
    public void testCreateTable() throws SQLException {
        try (JQuickSQLFactory factory = new JQuickSQLFactory(testConnector)) {
            boolean result = factory.createTable(testTableDefinition);
            assertTrue(result);
            // 验证表存在（通过查询不抛异常）
            List<JQuickRow> rows = factory.selectAll();
            assertNotNull(rows);
        }
    }

    @Test
    public void testCreateTableWithDefault() throws SQLException {
        try (JQuickSQLFactory factory = new JQuickSQLFactory(testConnector, testTableDefinition)) {
            boolean result = factory.createTable();
            assertTrue(result);
        }
    }


    @Test
    public void testCreateIndex() throws SQLException {
        try (JQuickSQLFactory factory = new JQuickSQLFactory(testConnector)) {
            factory.createTable(testTableDefinition);
            JQuickIndexDefinition index = new JQuickIndexDefinition();
            index.setIndexName("idx_test_user_name");
            index.setColumns(Collections.singletonList("name"));
            index.setUnique(false);
            boolean result = factory.createIndex("test_user", index);
            assertTrue(result);
        }
    }

    @Test
    public void testInsert() throws SQLException {
        try (JQuickSQLFactory factory = new JQuickSQLFactory(testConnector)) {
            factory.createTable(testTableDefinition);
            JQuickRow row = createTestRow("王五", 28, "wangwu@test.com");
            int result = factory.insert(row);
            assertEquals(1, result);
            long count = factory.countAll();
            assertEquals(1, count);
        }
    }

    @Test
    public void testInsertWithDefaultTable() throws SQLException {
        try (JQuickSQLFactory factory = new JQuickSQLFactory(testConnector, testTableDefinition)) {
            factory.createTable();
            JQuickRow row = createTestRow("赵六", 32, "zhaoliu@test.com");
            int result = factory.insert(row);
            assertEquals(1, result);
        }
    }

    @Test
    public void testInsertBatch() throws SQLException {
        try (JQuickSQLFactory factory = new JQuickSQLFactory(testConnector, testTableDefinition)) {
            factory.createTable();
            List<JQuickRow> rows = Arrays.asList(
                    createTestRow("批量1", 20, "batch1@test.com"),
                    createTestRow("批量2", 21, "batch2@test.com"),
                    createTestRow("批量3", 22, "batch3@test.com")
            );
            int[] results = factory.insertBatch(rows);
            assertEquals(3, results.length);
            long count = factory.countAll();
            assertEquals(3, count);
        }
    }

    @Test
    public void testUpdate() throws SQLException {
        try (JQuickSQLFactory factory = new JQuickSQLFactory(testConnector, testTableDefinition)) {
            factory.createTable();
            // 插入数据
            JQuickRow row = createTestRow("原始姓名", 25, "update@test.com");
            factory.insert(row);
            // 更新数据
            JQuickRow updateRow = new JQuickRow();
            updateRow.put("name", "更新后姓名");
            updateRow.put("age", 30);
            int result = factory.update(updateRow, "email = 'update@test.com'");
            assertEquals(1, result);
            // 验证更新结果
            JQuickRow updatedRow = factory.selectOne("email = 'update@test.com'");
            assertEquals("更新后姓名", updatedRow.getString("name"));
            assertEquals(Integer.valueOf(30), updatedRow.getInt("age"));
        }
    }

    @Test
    public void testDelete() throws SQLException {
        try (JQuickSQLFactory factory = new JQuickSQLFactory(testConnector, testTableDefinition)) {
            factory.createTable();
            // 插入数据
            factory.insert(createTestRow("删除测试1", 20, "delete1@test.com"));
            factory.insert(createTestRow("删除测试2", 21, "delete2@test.com"));
            long beforeCount = factory.countAll();
            assertEquals(2, beforeCount);
            // 删除数据
            int result = factory.delete("email = 'delete1@test.com'");
            assertEquals(1, result);
            long afterCount = factory.countAll();
            assertEquals(1, afterCount);
        }
    }

    @Test
    public void testDeleteAll() throws SQLException {
        try (JQuickSQLFactory factory = new JQuickSQLFactory(testConnector, testTableDefinition)) {
            factory.createTable();
            // 插入多条数据
            factory.insert(createTestRow("删除全部1", 20, "delall1@test.com"));
            factory.insert(createTestRow("删除全部2", 21, "delall2@test.com"));
            factory.insert(createTestRow("删除全部3", 22, "delall3@test.com"));
            long beforeCount = factory.countAll();
            assertEquals(3, beforeCount);
            // 删除所有数据
            int result = factory.deleteAll();
            assertEquals(3, result);
            long afterCount = factory.countAll();
            assertEquals(0, afterCount);
        }
    }

    @Test
    public void testSelectAll() throws SQLException {
        try (JQuickSQLFactory factory = new JQuickSQLFactory(testConnector, testTableDefinition)) {
            factory.createTable();
            // 插入测试数据
            factory.insert(createTestRow("查询1", 20, "select1@test.com"));
            factory.insert(createTestRow("查询2", 21, "select2@test.com"));
            factory.insert(createTestRow("查询3", 22, "select3@test.com"));
            List<JQuickRow> results = factory.selectAll();
            assertEquals(3, results.size());
        }
    }

    @Test
    public void testSelectWithCondition() throws SQLException {
        try (JQuickSQLFactory factory = new JQuickSQLFactory(testConnector, testTableDefinition)) {
            factory.createTable();

            factory.insert(createTestRow("条件1", 18, "cond1@test.com"));
            factory.insert(createTestRow("条件2", 25, "cond2@test.com"));
            factory.insert(createTestRow("条件3", 30, "cond3@test.com"));

            List<JQuickRow> results = factory.select(Arrays.asList("name", "age"), "age > 20");
            assertEquals(2, results.size());

            for (JQuickRow row : results) {
                assertTrue(row.getInt("age") > 20);
            }
        }
    }

    @Test
    public void testSelectOne() throws SQLException {
        try (JQuickSQLFactory factory = new JQuickSQLFactory(testConnector, testTableDefinition)) {
            factory.createTable();

            JQuickRow inserted = createTestRow("唯一查询", 25, "unique@test.com");
            factory.insert(inserted);

            JQuickRow result = factory.selectOne("email = 'unique@test.com'");
            assertNotNull(result);
            assertEquals("唯一查询", result.getString("name"));
            assertEquals(Integer.valueOf(25), result.getInt("age"));
        }
    }

    @Test
    public void testSelectById() throws SQLException {
        try (JQuickSQLFactory factory = new JQuickSQLFactory(testConnector, testTableDefinition)) {
            factory.createTable();
            JQuickRow row = createTestRow("ID查询", 25, "idquery@test.com");
            factory.insert(row);
            List<JQuickRow> all = factory.selectAll();
            if (!all.isEmpty()) {
                JQuickRow result = factory.selectById("id", all.get(0).get("id"));
                assertNotNull(result);
            }
        }
    }

    @Test
    public void testCount() throws SQLException {
        try (JQuickSQLFactory factory = new JQuickSQLFactory(testConnector, testTableDefinition)) {
            factory.createTable();
            factory.insert(createTestRow("统计1", 20, "count1@test.com"));
            factory.insert(createTestRow("统计2", 21, "count2@test.com"));
            factory.insert(createTestRow("统计3", 22, "count3@test.com"));
            long total = factory.countAll();
            assertEquals(3, total);
            long filtered = factory.count("age > 20");
            assertEquals(2, filtered);
        }
    }

    @Test
    public void testExists() throws SQLException {
        try (JQuickSQLFactory factory = new JQuickSQLFactory(testConnector, testTableDefinition)) {
            factory.createTable();

            factory.insert(createTestRow("存在测试", 25, "exists@test.com"));

            assertTrue(factory.exists("email = 'exists@test.com'"));
            assertFalse(factory.exists("email = 'notexists@test.com'"));
        }
    }

    @Test
    public void testExecuteUpdate() throws SQLException {
        try (JQuickSQLFactory factory = new JQuickSQLFactory(testConnector)) {
            factory.createTable(testTableDefinition);
            String sql = "INSERT INTO test_user (name, age, email) VALUES ('原生SQL', 25, 'native@test.com')";
            int result = factory.executeUpdate(sql);
            assertEquals(1, result);
        }
    }

    @Test
    public void testExecuteQuery() throws SQLException {
        try (JQuickSQLFactory factory = new JQuickSQLFactory(testConnector, testTableDefinition)) {
            factory.createTable();
            factory.insert(createTestRow("查询测试", 25, "query@test.com"));
            String sql = "SELECT * FROM test_user WHERE name = '查询测试'";
            List<JQuickRow> results = factory.executeQueryForRows(sql);
            assertEquals(1, results.size());
            assertEquals("查询测试", results.get(0).getString("name"));
        }
    }

    @Test
    public void testExecuteBatch() throws SQLException {
        try (JQuickSQLFactory factory = new JQuickSQLFactory(testConnector, testTableDefinition)) {
            factory.createTable();

            List<String> sqls = Arrays.asList(
                    "INSERT INTO test_user (name, age, email) VALUES ('批量1', 20, 'batch1@test.com')",
                    "INSERT INTO test_user (name, age, email) VALUES ('批量2', 21, 'batch2@test.com')",
                    "INSERT INTO test_user (name, age, email) VALUES ('批量3', 22, 'batch3@test.com')"
            );

            int[] results = factory.executeBatch(sqls);
            assertEquals(3, results.length);

            long count = factory.countAll();
            assertEquals(3, count);
        }
    }

    @Test
    public void testAccessDialect() {
        JQuickAccessDialect dialect = new JQuickAccessDialect();

        // 测试建表语句生成
        String createSql = dialect.buildCreateTable(testTableDefinition);
        assertNotNull(createSql);
        assertTrue(createSql.contains("CREATE TABLE"));
        assertTrue(createSql.contains("test_user"));

        // 测试插入语句生成
        JQuickRow row = createTestRow("Access测试", 25, "access@test.com");
        String insertSql = dialect.buildInsert(testTableDefinition, row);
        assertNotNull(insertSql);
        assertTrue(insertSql.contains("INSERT INTO"));
    }

    @Test
    public void testBigQueryDialect() {
        JQuickBigQueryDialect dialect = new JQuickBigQueryDialect();

        // 测试建表语句生成
        String createSql = dialect.buildCreateTable(testTableDefinition);
        assertNotNull(createSql);
        assertTrue(createSql.contains("CREATE TABLE"));

        // 测试分区配置
        Map<String, Object> extensions = new HashMap<>();
        extensions.put("partitionBy", "created_time");
        extensions.put("partitionType", "DAY");
        testTableDefinition.setExtensions(extensions);

        String partitionedSql = dialect.buildCreateTable(testTableDefinition);
        assertNotNull(partitionedSql);
    }

    @Test(expected = IllegalStateException.class)
    public void testCreateTableWithoutDefault() throws SQLException {
        try (JQuickSQLFactory factory = new JQuickSQLFactory(testConnector)) {
            factory.createTable(); // 应该抛出异常
        }
    }

    @Test(expected = IllegalStateException.class)
    public void testInsertWithoutDefault() throws SQLException {
        try (JQuickSQLFactory factory = new JQuickSQLFactory(testConnector)) {
            factory.insert(new JQuickRow()); // 应该抛出异常
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void testInvalidDatabaseType() {
        new JQuickSQLFactory(testConnector, "invalid_type", null);
    }



    @Test
    public void testTransactionWithMultipleOperations() throws SQLException {
        try (JQuickSQLFactory factory = new JQuickSQLFactory(testConnector, testTableDefinition)) {
            factory.createTable();

            // 开启事务
            factory.beginTransaction();

            try {
                // 执行多个操作
                factory.insert(createTestRow("事务测试1", 20, "tx1@test.com"));
                factory.insert(createTestRow("事务测试2", 21, "tx2@test.com"));
                factory.update(new JQuickRow() {{
                    put("age", 25);
                }}, "name = '事务测试1'");

                // 提交事务
                factory.commit();

                // 验证所有操作生效
                long count = factory.countAll();
                assertEquals(2, count);

                JQuickRow updated = factory.selectOne("name = '事务测试1'");
                assertEquals(Integer.valueOf(25), updated.getInt("age"));

            } catch (SQLException e) {
                factory.rollback();
                throw e;
            }
        }
    }
}
