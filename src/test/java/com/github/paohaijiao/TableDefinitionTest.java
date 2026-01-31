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

/**
 * packageName com.github.paohaijiao
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/1/30
 */
import com.github.paohaijiao.ddl.column.JQuickColumnDefinition;
import com.github.paohaijiao.ddl.dialect.JQuickDatabaseDialect;
import com.github.paohaijiao.ddl.index.JQuickIndexDefinition;
import com.github.paohaijiao.ddl.table.JQuickTableDefinition;

import java.util.Arrays;
import java.util.HashMap;

public class TableDefinitionTest {

    public static void main(String[] args) {
        JQuickTableDefinition usersTable = createUsersTable();
        System.out.println(usersTable.toDDL());
        System.out.println();

        JQuickTableDefinition ordersTable = createOrdersTable();
        System.out.println(ordersTable.toDDL());
        System.out.println();
        JQuickTableDefinition productsTable = createProductsTable();
        System.out.println(productsTable.toDDL());
        System.out.println();
        JQuickTableDefinition oracleTable = createOracleTable();
        System.out.println(oracleTable.toDDL());
        System.out.println();
    }

    private static JQuickTableDefinition createUsersTable() {
        JQuickTableDefinition table = new JQuickTableDefinition();
        table.setTableName("users");

        JQuickColumnDefinition idCol = new JQuickColumnDefinition();
        idCol.setName("id");
        idCol.setType("BIGINT");
        idCol.setNullable(false);
        idCol.setAutoIncrement(true);
        idCol.setComment("用户ID");
        table.getColumns().add(idCol);

        JQuickColumnDefinition usernameCol = new JQuickColumnDefinition();
        usernameCol.setName("username");
        usernameCol.setType("VARCHAR");
        usernameCol.setLength(50);
        usernameCol.setNullable(false);
        usernameCol.setComment("用户名");
        HashMap<String,Object> map=new HashMap<String,Object>() ;
        map.put("COLLATE", "utf8mb4_bin");
        usernameCol.setColumnOptions(map);
        table.getColumns().add(usernameCol);

        JQuickColumnDefinition emailCol = new JQuickColumnDefinition();
        emailCol.setName("email");
        emailCol.setType("VARCHAR");
        emailCol.setLength(100);
        emailCol.setNullable(false);
        emailCol.setComment("邮箱");
        table.getColumns().add(emailCol);

        JQuickColumnDefinition passwordCol = new JQuickColumnDefinition();
        passwordCol.setName("password_hash");
        passwordCol.setType("CHAR");
        passwordCol.setLength(64);
        passwordCol.setNullable(false);
        passwordCol.setComment("密码哈希");
        table.getColumns().add(passwordCol);

        JQuickColumnDefinition statusCol = new JQuickColumnDefinition();
        statusCol.setName("status");
        statusCol.setType("TINYINT");
        statusCol.setLength(1);
        statusCol.setNullable(false);
        statusCol.setDefaultValue("1");
        statusCol.setComment("状态：0-禁用，1-正常");
        table.getColumns().add(statusCol);

        JQuickColumnDefinition createTimeCol = new JQuickColumnDefinition();
        createTimeCol.setName("created_at");
        createTimeCol.setType("DATETIME");
        createTimeCol.setNullable(false);
        createTimeCol.setDefaultValue("CURRENT_TIMESTAMP");
        createTimeCol.setComment("创建时间");
        table.getColumns().add(createTimeCol);

        JQuickColumnDefinition updateTimeCol = new JQuickColumnDefinition();
        updateTimeCol.setName("updated_at");
        updateTimeCol.setType("DATETIME");
        updateTimeCol.setNullable(false);
        updateTimeCol.setDefaultValue("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP");
        updateTimeCol.setComment("更新时间");
        table.getColumns().add(updateTimeCol);

        table.getPrimaryKeys().add("id");

        JQuickIndexDefinition emailIdx = new JQuickIndexDefinition();
        emailIdx.setName("uk_email");
        emailIdx.setColumns(Arrays.asList("email"));
        emailIdx.setUnique(true);
        HashMap<String,Object> index=new HashMap<String,Object>() ;
        index. put("COMMENT", "邮箱唯一索引");
        emailIdx.setIndexOptions(index);
        table.getIndexes().add(emailIdx);

        JQuickIndexDefinition usernameIdx = new JQuickIndexDefinition();
        usernameIdx.setName("idx_username");
        usernameIdx.setColumns(Arrays.asList("username"));
        HashMap<String,Object> index1=new HashMap<String,Object>() ;
        index1. put("COMMENT", "邮箱唯一索引");
        usernameIdx.setIndexOptions(index1);
        table.getIndexes().add(usernameIdx);

        JQuickIndexDefinition statusIdx = new JQuickIndexDefinition();
        statusIdx.setName("idx_status");
        statusIdx.setColumns(Arrays.asList("status"));
        table.getIndexes().add(statusIdx);

        table.getTableOptions().put("ENGINE", "InnoDB");
        table.getTableOptions().put("CHARSET", "utf8mb4");
        table.getTableOptions().put("COLLATE", "utf8mb4_unicode_ci");
        table.getTableOptions().put("ROW_FORMAT", "DYNAMIC");
        table.getTableOptions().put("COMMENT", "用户表");

        return table;
    }

    private static JQuickTableDefinition createOrdersTable() {
        JQuickTableDefinition table = new JQuickTableDefinition();
        table.setTableName("orders");

        // 订单ID - 主键
        JQuickColumnDefinition idCol = new JQuickColumnDefinition();
        idCol.setName("order_id");
        idCol.setType("VARCHAR");
        idCol.setLength(32);
        idCol.setNullable(false);
        idCol.setComment("订单ID");
        table.getColumns().add(idCol);

        // 用户ID - 外键（简化表示）
        JQuickColumnDefinition userIdCol = new JQuickColumnDefinition();
        userIdCol.setName("user_id");
        userIdCol.setType("BIGINT");
        userIdCol.setNullable(false);
        userIdCol.setComment("用户ID");
        table.getColumns().add(userIdCol);

        // 订单金额
        JQuickColumnDefinition amountCol = new JQuickColumnDefinition();
        amountCol.setName("amount");
        amountCol.setType("DECIMAL");
        amountCol.setPrecision(12);
        amountCol.setScale(2);
        amountCol.setNullable(false);
        amountCol.setDefaultValue("0.00");
        amountCol.setComment("订单金额");
        table.getColumns().add(amountCol);

        // 订单状态
        JQuickColumnDefinition statusCol = new JQuickColumnDefinition();
        statusCol.setName("order_status");
        statusCol.setType("VARCHAR");
        statusCol.setLength(20);
        statusCol.setNullable(false);
        statusCol.setDefaultValue("'pending'");
        statusCol.setComment("订单状态");
        table.getColumns().add(statusCol);

        // 创建时间
        JQuickColumnDefinition createTimeCol = new JQuickColumnDefinition();
        createTimeCol.setName("created_at");
        createTimeCol.setType("TIMESTAMP");
        createTimeCol.setNullable(false);
        createTimeCol.setDefaultValue("CURRENT_TIMESTAMP");
        createTimeCol.setComment("创建时间");
        table.getColumns().add(createTimeCol);

        // 复合主键（示例：如果使用订单ID和用户ID作为复合主键）
        table.getPrimaryKeys().addAll(Arrays.asList("order_id", "user_id"));

        // 索引：用户ID索引
        JQuickIndexDefinition userIdIdx = new JQuickIndexDefinition();
        userIdIdx.setName("idx_user_id");
        userIdIdx.setColumns(Arrays.asList("user_id"));
        table.getIndexes().add(userIdIdx);

        // 索引：状态和创建时间的联合索引
        JQuickIndexDefinition statusCreateIdx = new JQuickIndexDefinition();
        statusCreateIdx.setName("idx_status_created");
        statusCreateIdx.setColumns(Arrays.asList("order_status", "created_at"));
        table.getIndexes().add(statusCreateIdx);

        // 表选项
        table.getTableOptions().put("ENGINE", "InnoDB");
        table.getTableOptions().put("CHARSET", "utf8mb4");
        table.getTableOptions().put("COMMENT", "订单表");

        return table;
    }

    private static JQuickTableDefinition createProductsTable() {
        JQuickTableDefinition table = new JQuickTableDefinition();
        table.setTableName("products");

        // 商品ID
        JQuickColumnDefinition idCol = new JQuickColumnDefinition();
        idCol.setName("product_id");
        idCol.setType("INT");
        idCol.setNullable(false);
        idCol.setAutoIncrement(true);
        idCol.setComment("商品ID");
        table.getColumns().add(idCol);

        // 商品名称
        JQuickColumnDefinition nameCol = new JQuickColumnDefinition();
        nameCol.setName("product_name");
        nameCol.setType("VARCHAR");
        nameCol.setLength(200);
        nameCol.setNullable(false);
        nameCol.setComment("商品名称");
        table.getColumns().add(nameCol);

        // 商品编码
        JQuickColumnDefinition codeCol = new JQuickColumnDefinition();
        codeCol.setName("product_code");
        codeCol.setType("VARCHAR");
        codeCol.setLength(50);
        codeCol.setNullable(false);
        codeCol.setComment("商品编码");
        table.getColumns().add(codeCol);

        // 价格
        JQuickColumnDefinition priceCol = new JQuickColumnDefinition();
        priceCol.setName("price");
        priceCol.setType("DECIMAL");
        priceCol.setPrecision(10);
        priceCol.setScale(2);
        priceCol.setNullable(false);
        priceCol.setComment("价格");
        table.getColumns().add(priceCol);

        // 库存
        JQuickColumnDefinition stockCol = new JQuickColumnDefinition();
        stockCol.setName("stock");
        stockCol.setType("INT");
        stockCol.setNullable(false);
        stockCol.setDefaultValue("0");
        stockCol.setComment("库存数量");
        table.getColumns().add(stockCol);

        // 分类ID
        JQuickColumnDefinition categoryCol = new JQuickColumnDefinition();
        categoryCol.setName("category_id");
        categoryCol.setType("INT");
        categoryCol.setNullable(false);
        categoryCol.setComment("分类ID");
        table.getColumns().add(categoryCol);

        // 设置主键
        table.getPrimaryKeys().add("product_id");

        // 商品编码唯一索引
        JQuickIndexDefinition codeIdx = new JQuickIndexDefinition();
        codeIdx.setName("uk_product_code");
        codeIdx.setColumns(Arrays.asList("product_code"));
        codeIdx.setUnique(true);
        table.getIndexes().add(codeIdx);

        // 分类ID和商品名称的联合索引
        JQuickIndexDefinition categoryNameIdx = new JQuickIndexDefinition();
        categoryNameIdx.setName("idx_category_name");
        categoryNameIdx.setColumns(Arrays.asList("category_id", "product_name"));
        table.getIndexes().add(categoryNameIdx);

        // 价格索引
        JQuickIndexDefinition priceIdx = new JQuickIndexDefinition();
        priceIdx.setName("idx_price");
        priceIdx.setColumns(Arrays.asList("price"));
        table.getIndexes().add(priceIdx);

        // 表选项
        table.getTableOptions().put("ENGINE", "InnoDB");
        table.getTableOptions().put("CHARSET", "utf8mb4");
        table.getTableOptions().put("AUTO_INCREMENT", "10001");
        table.getTableOptions().put("COMMENT", "商品表");

        return table;
    }

    private static JQuickTableDefinition createOracleTable() {
        JQuickTableDefinition table = new JQuickTableDefinition();
        table.setTableName("employees");
        table.setDialect(JQuickDatabaseDialect.ORACLE);

        // 员工ID
        JQuickColumnDefinition idCol = new JQuickColumnDefinition();
        idCol.setName("employee_id");
        idCol.setType("NUMBER");
        idCol.setPrecision(10);
        idCol.setNullable(false);
        idCol.setComment("员工ID");
        table.getColumns().add(idCol);

        // 员工姓名
        JQuickColumnDefinition nameCol = new JQuickColumnDefinition();
        nameCol.setName("employee_name");
        nameCol.setType("VARCHAR2");
        nameCol.setLength(100);
        nameCol.setNullable(false);
        nameCol.setComment("员工姓名");
        table.getColumns().add(nameCol);

        // 部门ID
        JQuickColumnDefinition deptCol = new JQuickColumnDefinition();
        deptCol.setName("department_id");
        deptCol.setType("NUMBER");
        deptCol.setPrecision(5);
        deptCol.setNullable(false);
        deptCol.setComment("部门ID");
        table.getColumns().add(deptCol);

        // 工资
        JQuickColumnDefinition salaryCol = new JQuickColumnDefinition();
        salaryCol.setName("salary");
        salaryCol.setType("NUMBER");
        salaryCol.setPrecision(10);
        salaryCol.setScale(2);
        salaryCol.setNullable(true);
        salaryCol.setComment("工资");
        table.getColumns().add(salaryCol);

        // 入职日期
        JQuickColumnDefinition hireDateCol = new JQuickColumnDefinition();
        hireDateCol.setName("hire_date");
        hireDateCol.setType("DATE");
        hireDateCol.setNullable(false);
        hireDateCol.setDefaultValue("SYSDATE");
        hireDateCol.setComment("入职日期");
        table.getColumns().add(hireDateCol);

        // 设置主键
        table.getPrimaryKeys().add("employee_id");

        // 部门ID索引
        JQuickIndexDefinition deptIdx = new JQuickIndexDefinition();
        deptIdx.setName("idx_department_id");
        deptIdx.setColumns(Arrays.asList("department_id"));
        table.getIndexes().add(deptIdx);

        // 表选项（Oracle特定）
        table.getTableOptions().put("TABLESPACE", "USERS");
        table.getTableOptions().put("PCTFREE", "10");
        table.getTableOptions().put("INITRANS", "2");

        return table;
    }
}
