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
package com.github.paohaijiao.transaction;

import com.github.paohaijiao.context.JQuickConnectionContext;

import java.sql.Connection;
import java.sql.SQLException;

/**
 * packageName com.github.paohaijiao.transaction
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/16
 */
public class TransactionService {
    public void updateOrder() throws SQLException {
        try {
//            Connection conn = DruidUtil.getConnection();
//            JQuickConnectionContext.setConnection(conn);
//            JQuickConnectionContext.beginTransaction();
//            orderDao.update(order);
//            itemDao.update(order.getItems());
//            JQuickConnectionContext.commit();
        } catch (Exception e) {
            JQuickConnectionContext.rollback();
            e.printStackTrace();
        } finally {
            JQuickConnectionContext.close();
        }
    }

}
