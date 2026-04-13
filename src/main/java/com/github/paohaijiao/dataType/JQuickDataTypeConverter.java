package com.github.paohaijiao.dataType;

import com.github.paohaijiao.dataType.enums.JQuickDataTypeFamily;

public interface JQuickDataTypeConverter {

    String convert(JQuickDataTypeFamily family, JQuickDataType dataType);

}
