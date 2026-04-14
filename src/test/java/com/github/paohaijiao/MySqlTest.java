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
import com.github.paohaijiao.dialect.JQuickSQLDialect;
import com.github.paohaijiao.extra.JQuickIndexDefinition;
import com.github.paohaijiao.extra.JQuickPrimaryKeyConstraint;
import com.github.paohaijiao.extra.JQuickUniqueConstraint;
import com.github.paohaijiao.manager.JQuickDatabaseTypeManager;
import com.github.paohaijiao.table.JQuickTableDefinition;
import org.junit.Test;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

/**
 * packageName com.github.paohaijiao
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/14
 */
public class MySqlTest {
    @Test
    public void testBuildCompleteCreateTable() {
        JQuickTableDefinition table = new JQuickTableDefinition();
        table.setTableName("test_order");
        table.setComment("订单表");
        JQuickColumnDefinition idCol = createColumn("id", JQuickDataTypeFamily.LONG);
        idCol.setNullable(false);
        idCol.setAutoIncrement(true);
        idCol.setComment("主键ID");
        table.getColumns().add(idCol);

        JQuickColumnDefinition orderNoCol = createColumn("order_no", JQuickDataTypeFamily.STRING);
        orderNoCol.getDataType().getParameters().put("length", 32);
        orderNoCol.setNullable(false);
        orderNoCol.setComment("订单号");
        table.getColumns().add(orderNoCol);

        JQuickColumnDefinition userIdCol = createColumn("user_id", JQuickDataTypeFamily.LONG);
        userIdCol.setNullable(false);
        userIdCol.setComment("用户ID");
        table.getColumns().add(userIdCol);

        JQuickColumnDefinition amountCol = createColumn("amount", JQuickDataTypeFamily.DECIMAL);
        amountCol.getDataType().getParameters().put("precision", 10);
        amountCol.getDataType().getParameters().put("scale", 2);
        amountCol.setDefaultValue(0);
        amountCol.setComment("订单金额");
        table.getColumns().add(amountCol);

        JQuickColumnDefinition statusCol = createColumn("status", JQuickDataTypeFamily.STRING);
        statusCol.getDataType().getParameters().put("length", 20);
        statusCol.setDefaultValue("pending");
        statusCol.setComment("订单状态");
        table.getColumns().add(statusCol);

        JQuickColumnDefinition createdAtCol = createColumn("created_at", JQuickDataTypeFamily.TIMESTAMP);
        createdAtCol.setDefaultValue("CURRENT_TIMESTAMP");
        createdAtCol.setComment("创建时间");
        table.getColumns().add(createdAtCol);

        // 主键
        JQuickPrimaryKeyConstraint pk = new JQuickPrimaryKeyConstraint();
        pk.setConstraintName("pk_test_order");
        pk.setColumns(Collections.singletonList("id"));
        table.getPrimaryKeys().add(pk);

        // 唯一约束
        JQuickUniqueConstraint uc = new JQuickUniqueConstraint();
        uc.setConstraintName("uk_order_no");
        uc.setColumns(Collections.singletonList("order_no"));
        table.getUniqueConstraints().add(uc);

        // 索引
        JQuickIndexDefinition idxUserId = new JQuickIndexDefinition();
        idxUserId.setIndexName("idx_user_id");
        idxUserId.setColumns(Collections.singletonList("user_id"));
        idxUserId.setType("BTREE");
        table.getIndexes().add(idxUserId);

        JQuickIndexDefinition idxStatus = new JQuickIndexDefinition();
        idxStatus.setIndexName("idx_status");
        idxStatus.setColumns(Collections.singletonList("status"));
        table.getIndexes().add(idxStatus);

        // 扩展选项
        Map<String, Object> extensions = new HashMap<>();
        extensions.put("ENGINE", "InnoDB");
        extensions.put("CHARSET", "utf8mb4");
        table.setExtensions(extensions);
        JQuickDatabaseTypeManager manager = JQuickDatabaseTypeManager.getInstance();
        JQuickSQLDialect dialect = manager.getDialect("mysql");
       // JQuickSQLDialect dialect = manager.getDialectByJdbcUrl("jdbc:mysql://localhost:3306/test");

        String sql = dialect.buildCreateTable(table);
        System.out.println(sql);
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

}
