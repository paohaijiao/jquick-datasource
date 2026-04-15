package com.github.paohaijiao.dialect;

import com.github.paohaijiao.column.JQuickColumnDefinition;
import com.github.paohaijiao.dataType.JQuickDataType;
import com.github.paohaijiao.extra.JQuickForeignKeyConstraint;
import com.github.paohaijiao.extra.JQuickIndexDefinition;
import com.github.paohaijiao.extra.JQuickPrimaryKeyConstraint;
import com.github.paohaijiao.extra.JQuickUniqueConstraint;
import com.github.paohaijiao.row.JQuickRow;
import com.github.paohaijiao.table.JQuickTableDefinition;

import java.util.List;

public interface JQuickSQLDialect {

    String buildCreateTable(JQuickTableDefinition table);

    String buildColumnDefinition(JQuickColumnDefinition column);

    String buildPrimaryKey(JQuickPrimaryKeyConstraint pk);

    String buildUniqueConstraint(JQuickUniqueConstraint uc);

    String buildIndex(JQuickIndexDefinition index);

    String buildForeignKey(JQuickForeignKeyConstraint fk);

    String buildComment(JQuickColumnDefinition column);

    String getAutoIncrementKeyword();

    String getDataTypeString(JQuickDataType dataType);


    String buildModifyColumn(String tableName, JQuickColumnDefinition column);

    String buildChangeColumn(String tableName, JQuickColumnDefinition column);

    String buildShowCreateTable(String tableName, JQuickColumnDefinition column);


    String buildDescribeTable(String tableName, JQuickColumnDefinition column);

    String buildDml(JQuickRow tableName, JQuickColumnDefinition column);

}
