package com.github.paohaijiao.abs;
import com.github.paohaijiao.ddl.column.JQuickColumnDefinition;
import com.github.paohaijiao.ddl.index.JQuickIndexDefinition;
import com.github.paohaijiao.ddl.table.JQuickTableDefinition;

import java.util.List;
import java.util.Map;

public interface TableBuilder {

    String buildCreateTable(JQuickTableDefinition tableDefinition);

    String buildColumnDefinition(JQuickColumnDefinition column);

    String buildPrimaryKeyConstraint(List<String> primaryKeys);

    String buildIndexDefinition(JQuickIndexDefinition index);

    String buildTableOptions(Map<String, Object> tableOptions);
}
