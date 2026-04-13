package com.github.paohaijiao.dialect;

import com.github.paohaijiao.column.JQuickColumnDefinition;
import com.github.paohaijiao.dataType.JQuickDataType;
import com.github.paohaijiao.extra.JQuickForeignKeyConstraint;
import com.github.paohaijiao.extra.JQuickIndexDefinition;
import com.github.paohaijiao.extra.JQuickPrimaryKeyConstraint;
import com.github.paohaijiao.extra.JQuickUniqueConstraint;
import com.github.paohaijiao.table.JQuickTableDefinition;

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
}
