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

    String buildChangeColumn(String tableName, String oldName,JQuickColumnDefinition column);

    String buildShowCreateTable(String tableName);


    String buildDescribeTable(String tableName);

    String buildInsert(JQuickRow row, JQuickTableDefinition table);

    String buildUpdate(JQuickRow row, JQuickTableDefinition table, String whereClause);

    String buildDelete(JQuickTableDefinition table, String whereClause);

    String buildSelect(JQuickTableDefinition table, List<String> columns, String whereClause);

}
