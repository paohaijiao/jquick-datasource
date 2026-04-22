package com.github.paohaijiao.dialect;

import com.github.paohaijiao.column.JQuickColumnDefinition;
import com.github.paohaijiao.connector.JQuickDataSourceConnector;
import com.github.paohaijiao.dataType.JQuickDataType;
import com.github.paohaijiao.extra.JQuickForeignKeyConstraint;
import com.github.paohaijiao.extra.JQuickIndexDefinition;
import com.github.paohaijiao.extra.JQuickPrimaryKeyConstraint;
import com.github.paohaijiao.extra.JQuickUniqueConstraint;
import com.github.paohaijiao.statement.JQuickRow;
import com.github.paohaijiao.table.JQuickTableDefinition;

import java.sql.Connection;
import java.sql.Statement;
import java.util.List;

public interface JQuickSQLDialect {

    String buildCreateTable(JQuickTableDefinition table);

    String buildColumnDefinition(JQuickTableDefinition tableDefinition,JQuickColumnDefinition column);

    String buildPrimaryKey(JQuickTableDefinition tableDefinition,JQuickPrimaryKeyConstraint pk);

    String buildUniqueConstraint(JQuickTableDefinition tableDefinition,JQuickUniqueConstraint uc);

    String buildIndex(JQuickTableDefinition tableDefinition,JQuickIndexDefinition index);

    String buildForeignKey(JQuickTableDefinition tableDefinition,JQuickForeignKeyConstraint fk);

    String buildComment(JQuickTableDefinition tableDefinition,JQuickColumnDefinition column);

    String getAutoIncrementKeyword(JQuickTableDefinition tableDefinition);

    String getDataTypeString(JQuickTableDefinition tableDefinition,JQuickDataType dataType);


    String buildModifyColumn(JQuickTableDefinition tableDefinition,String tableName, JQuickColumnDefinition column);

    String buildChangeColumn(JQuickTableDefinition tableDefinition,String tableName, String oldName,JQuickColumnDefinition column);

    String buildShowCreateTable(JQuickTableDefinition tableDefinition,String tableName);


    String buildDescribeTable(JQuickTableDefinition tableDefinition,String tableName);

    String buildInsert(JQuickTableDefinition tableDefinition, JQuickRow row);

    String buildUpdate(JQuickTableDefinition tableDefinition,JQuickRow row, String whereClause);

    String buildDelete(JQuickTableDefinition tableDefinition, String whereClause);

    String buildSelect(JQuickTableDefinition tableDefinition,List<String> columns, String whereClause);

    String getDriverClass(JQuickDataSourceConnector connector);

    String getUrl(JQuickDataSourceConnector connector);

    Connection getConnection(JQuickDataSourceConnector connector);

}
