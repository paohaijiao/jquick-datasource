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
package com.github.paohaijiao;

import com.github.paohaijiao.column.JQuickColumnDefinition;
import com.github.paohaijiao.dataType.JQuickDataType;
import com.github.paohaijiao.dataType.enums.JQuickDataTypeFamily;
import com.github.paohaijiao.extra.JQuickPrimaryKeyConstraint;
import com.github.paohaijiao.extra.JQuickUniqueConstraint;
import com.github.paohaijiao.impl.JQuickGaussDBDialect;
import com.github.paohaijiao.impl.JQuickInfluxDBDialect;
import com.github.paohaijiao.row.JQuickRow;
import com.github.paohaijiao.table.JQuickTableDefinition;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;

/**
 * packageName com.github.paohaijiao
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/14
 */
public class InfluxDbSqlTest {
    @Test
    public    void testBuildCreateTable() {
        JQuickTableDefinition  tableDefinition = new JQuickTableDefinition();
        tableDefinition.setTableName("test_user");
        tableDefinition.setComment("用户信息表");
        tableDefinition.setQuoteEnabled(true);

        JQuickColumnDefinition idColumn = createColumn("id", JQuickDataTypeFamily.LONG);
        idColumn.setAutoIncrement(true);
        idColumn.setNullable(false);

        JQuickColumnDefinition nameColumn = createColumn("name", JQuickDataTypeFamily.STRING);
        nameColumn.setLength(100);
        nameColumn.setNullable(false);
        nameColumn.setComment("用户姓名");

        JQuickColumnDefinition ageColumn = createColumn("age", JQuickDataTypeFamily.INTEGER);
        ageColumn.setComment("年龄");

        JQuickColumnDefinition emailColumn = createColumn("email", JQuickDataTypeFamily.STRING);
        emailColumn.setLength(200);
        emailColumn.setNullable(false);
        emailColumn.setDefaultValue("unknown@example.com");

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
        JQuickInfluxDBDialect dialect = new JQuickInfluxDBDialect();
        String sql = dialect.buildCreateTable(tableDefinition);
        System.out.println("建表语句: \n" + sql);
    }
    private JQuickColumnDefinition createColumn(String name, JQuickDataTypeFamily family) {
        JQuickColumnDefinition column = new JQuickColumnDefinition();
        column.setColumnName(name);
        JQuickDataType dataType = new JQuickDataType();
        dataType.setFamily(family);
        dataType.setParameters(new HashMap<>());
        column.setDataType(dataType);

        return column;
    }
    @Test
    public void testBuildInsert() {
        JQuickTableDefinition  tableDefinition = new JQuickTableDefinition();
        tableDefinition.setTableName("test_user");
        tableDefinition.setComment("用户信息表");
        tableDefinition.setQuoteEnabled(true);
        JQuickRow row = new JQuickRow();
        row.put("name", "张三");
        row.put("age", 25);
        row.put("email", "zhangsan@example.com");
        JQuickInfluxDBDialect  dialect = new JQuickInfluxDBDialect();
        String sql = dialect.buildInsert(tableDefinition, row);
        System.out.println("INSERT 语句: " + sql);
    }
    @Test
    public    void testBuildUpdate() {
        JQuickTableDefinition  tableDefinition = new JQuickTableDefinition();
        tableDefinition.setTableName("test_user");
        tableDefinition.setComment("用户信息表");
        tableDefinition.setQuoteEnabled(true);
        JQuickRow row = new JQuickRow();
        row.put("name", "王五");
        row.put("age", 28);
        String whereClause = "\"id\" = 1";
        JQuickInfluxDBDialect  dialect = new JQuickInfluxDBDialect();
        String sql = dialect.buildUpdate(tableDefinition, row, whereClause);

        System.out.println("UPDATE 语句: " + sql);
    }
    @Test
    public     void testBuildDelete() {
        JQuickTableDefinition  tableDefinition = new JQuickTableDefinition();
        tableDefinition.setTableName("test_user");
        tableDefinition.setComment("用户信息表");
        tableDefinition.setQuoteEnabled(true);
        String whereClause = "\"id\" = 1";
        JQuickInfluxDBDialect  dialect = new JQuickInfluxDBDialect();
        String sql = dialect.buildDelete(tableDefinition, whereClause);
        System.out.println("DELETE 语句: " + sql);
    }
}
