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
package com.github.paohaijiao.dataType;

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

import com.github.paohaijiao.dataType.enums.JQuickDataTypeFamily;
import com.github.paohaijiao.dataType.impl.JQuickDefaultDataTypeConverter;
import com.github.paohaijiao.dataType.impl.JQuickKingbaseESDataTypeConverter;
import com.github.paohaijiao.dataType.impl.JQuickMariaDbSQLDataTypeConverter;
import com.github.paohaijiao.dataType.impl.JQuickMySQLDataTypeConverter;
import com.github.paohaijiao.dialect.JQuickSQLDialect;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 数据类型家族管理器
 * 根据不同的数据库方言和参数将 DataTypeFamily 转换为具体的数据库类型字符串
 *
 * @author Martin
 * @version 1.0.0
 * @since 2026/4/13
 */
public class JQuickDataTypeFamilyManager {

    private static volatile JQuickDataTypeFamilyManager instance;

    private final Map<String, JQuickDataTypeConverter> converterMap;

    private final Map<JQuickDataTypeFamily, TypeParameterDefaults> defaultParameters;

    private final Map<Class<?>, JQuickDataTypeFamily> javaTypeToFamilyMap;

    private JQuickDataTypeFamilyManager() {
        this.converterMap = new ConcurrentHashMap<>();
        this.defaultParameters = new ConcurrentHashMap<>();
        this.javaTypeToFamilyMap = new ConcurrentHashMap<>();
        initDefaultConverters();
        initDefaultParameters();
        initJavaTypeMapping();
    }

    /**
     * 获取单例实例
     */
    public static JQuickDataTypeFamilyManager getInstance() {
        if (instance == null) {
            synchronized (JQuickDataTypeFamilyManager.class) {
                if (instance == null) {
                    instance = new JQuickDataTypeFamilyManager();
                }
            }
        }
        return instance;
    }

    /**
     * 初始化默认的类型转换器
     */
    private void initDefaultConverters() {
        registerConverter("mysql", new JQuickMySQLDataTypeConverter());
        registerConverter("mariadb", new JQuickMariaDbSQLDataTypeConverter());
        registerConverter("kingbasees", new JQuickKingbaseESDataTypeConverter());
        registerConverter("postgresql", new JQuickKingbaseESDataTypeConverter());
        registerConverter("default", new JQuickDefaultDataTypeConverter());
    }

    /**
     * 初始化默认参数
     */
    private void initDefaultParameters() {
        defaultParameters.put(JQuickDataTypeFamily.STRING, TypeParameterDefaults.builder().length(255).build());

        // 整数类型默认参数
        defaultParameters.put(JQuickDataTypeFamily.INTEGER, TypeParameterDefaults.builder().build());
        defaultParameters.put(JQuickDataTypeFamily.LONG, TypeParameterDefaults.builder().build());
        defaultParameters.put(JQuickDataTypeFamily.SHORT, TypeParameterDefaults.builder().build());
        defaultParameters.put(JQuickDataTypeFamily.BYTE, TypeParameterDefaults.builder().build());

        // 小数类型默认参数
        defaultParameters.put(JQuickDataTypeFamily.DECIMAL, TypeParameterDefaults.builder().precision(10).scale(0).build());

        // 时间戳默认参数
        defaultParameters.put(JQuickDataTypeFamily.TIMESTAMP, TypeParameterDefaults.builder().precision(0).build());

        // 二进制类型默认参数
        defaultParameters.put(JQuickDataTypeFamily.BINARY, TypeParameterDefaults.builder().length(1).build());
        defaultParameters.put(JQuickDataTypeFamily.VARBINARY, TypeParameterDefaults.builder().length(255).build());
    }

    /**
     * 初始化 Java 类型到 DataTypeFamily 的映射
     */
    private void initJavaTypeMapping() {
        javaTypeToFamilyMap.put(String.class, JQuickDataTypeFamily.STRING);
        javaTypeToFamilyMap.put(char[].class, JQuickDataTypeFamily.STRING);
        javaTypeToFamilyMap.put(CharSequence.class, JQuickDataTypeFamily.STRING);

        javaTypeToFamilyMap.put(Integer.class, JQuickDataTypeFamily.INTEGER);
        javaTypeToFamilyMap.put(int.class, JQuickDataTypeFamily.INTEGER);
        javaTypeToFamilyMap.put(Long.class, JQuickDataTypeFamily.LONG);
        javaTypeToFamilyMap.put(long.class, JQuickDataTypeFamily.LONG);
        javaTypeToFamilyMap.put(Short.class, JQuickDataTypeFamily.SHORT);
        javaTypeToFamilyMap.put(short.class, JQuickDataTypeFamily.SHORT);
        javaTypeToFamilyMap.put(Byte.class, JQuickDataTypeFamily.BYTE);
        javaTypeToFamilyMap.put(byte.class, JQuickDataTypeFamily.BYTE);
        javaTypeToFamilyMap.put(BigInteger.class, JQuickDataTypeFamily.LONG);

        javaTypeToFamilyMap.put(Float.class, JQuickDataTypeFamily.FLOAT);
        javaTypeToFamilyMap.put(float.class, JQuickDataTypeFamily.FLOAT);
        javaTypeToFamilyMap.put(Double.class, JQuickDataTypeFamily.DOUBLE);
        javaTypeToFamilyMap.put(double.class, JQuickDataTypeFamily.DOUBLE);
        javaTypeToFamilyMap.put(BigDecimal.class, JQuickDataTypeFamily.DECIMAL);

        javaTypeToFamilyMap.put(java.sql.Date.class, JQuickDataTypeFamily.DATE);
        javaTypeToFamilyMap.put(LocalDate.class, JQuickDataTypeFamily.DATE);
        javaTypeToFamilyMap.put(java.util.Date.class, JQuickDataTypeFamily.TIMESTAMP);
        javaTypeToFamilyMap.put(LocalDateTime.class, JQuickDataTypeFamily.TIMESTAMP);
        javaTypeToFamilyMap.put(LocalTime.class, JQuickDataTypeFamily.TIME);
        javaTypeToFamilyMap.put(java.sql.Time.class, JQuickDataTypeFamily.TIME);
        javaTypeToFamilyMap.put(java.sql.Timestamp.class, JQuickDataTypeFamily.TIMESTAMP);

        javaTypeToFamilyMap.put(Boolean.class, JQuickDataTypeFamily.BOOLEAN);
        javaTypeToFamilyMap.put(boolean.class, JQuickDataTypeFamily.BOOLEAN);

        javaTypeToFamilyMap.put(byte[].class, JQuickDataTypeFamily.BLOB);

        javaTypeToFamilyMap.put(Map.class, JQuickDataTypeFamily.JSON);
        javaTypeToFamilyMap.put(List.class, JQuickDataTypeFamily.JSON);
    }

    /**
     * 注册类型转换器
     */
    public void registerConverter(String databaseType, JQuickDataTypeConverter converter) {
        if (databaseType == null || converter == null) {
            throw new IllegalArgumentException("databaseType and converter cannot be null");
        }
        converterMap.put(databaseType.toLowerCase(), converter);
    }

    /**
     * 获取类型转换器
     */
    public JQuickDataTypeConverter getConverter(String databaseType) {
        if (databaseType == null) {
            return converterMap.get("default");
        }
        String dbType = databaseType.toLowerCase();
        return converterMap.getOrDefault(dbType, converterMap.get("default"));
    }

    /**
     * 根据 SQLDialect 和 DataType 转换类型
     */
    public String convert(JQuickSQLDialect dialect, JQuickDataType dataType) {
        if (dialect == null || dataType == null) {
            return "VARCHAR(255)";
        }
        if (dataType.getCustomTypeName() != null && !dataType.getCustomTypeName().isEmpty()) {
            return dataType.getCustomTypeName();
        }
        JQuickDataTypeFamily family = dataType.getFamily();
        if (family == null) {
            return "VARCHAR(255)";
        }
        String databaseType = detectDatabaseType(dialect);
        JQuickDataTypeConverter converter = getConverter(databaseType);

        return converter.convert(family, dataType);
    }

    /**
     * 根据 SQLDialect 检测数据库类型
     */
    private String detectDatabaseType(JQuickSQLDialect dialect) {
        String className = dialect.getClass().getSimpleName().toLowerCase();
        if (className.contains("mysql")) {
            return "mysql";
        }
        if (className.contains("kingbase")) {
            return "kingbasees";
        }
        if (className.contains("postgresql")) {
            return "postgresql";
        }
        return "default";
    }

    /**
     * 根据 Java 类型推断 DataTypeFamily
     */
    public JQuickDataTypeFamily inferFamilyFromJavaType(Class<?> javaType) {
        if (javaType == null) {
            return JQuickDataTypeFamily.STRING;
        }
        JQuickDataTypeFamily family = javaTypeToFamilyMap.get(javaType);
        if (family != null) {
            return family;
        }
        if (javaType.isEnum()) {
            return JQuickDataTypeFamily.STRING;
        }
        if (javaType.isArray()) {
            Class<?> componentType = javaType.getComponentType();
            if (componentType == byte.class || componentType == Byte.class) {
                return JQuickDataTypeFamily.BLOB;
            }
            return JQuickDataTypeFamily.STRING;
        }

        return JQuickDataTypeFamily.STRING;
    }

    /**
     * 根据 Java 对象值推断 DataTypeFamily
     */
    public JQuickDataTypeFamily inferFamilyFromValue(Object value) {
        if (value == null) {
            return JQuickDataTypeFamily.STRING;
        }
        return inferFamilyFromJavaType(value.getClass());
    }

    /**
     * 创建 DataType 对象
     */
    public JQuickDataType createDataType(JQuickDataTypeFamily family) {
        JQuickDataType dataType = new JQuickDataType();
        dataType.setFamily(family);

        // 设置默认参数
        TypeParameterDefaults defaults = defaultParameters.get(family);
        if (defaults != null) {
            Map<String, Object> params = new HashMap<>();
            if (defaults.length != null) {
                params.put("length", defaults.length);
            }
            if (defaults.precision != null) {
                params.put("precision", defaults.precision);
            }
            if (defaults.scale != null) {
                params.put("scale", defaults.scale);
            }
            if (!params.isEmpty()) {
                dataType.setParameters(params);
            }
        }

        return dataType;
    }

    /**
     * 从 Java 类型创建 DataType
     */
    public JQuickDataType createDataTypeFromJavaType(Class<?> javaType) {
        JQuickDataTypeFamily family = inferFamilyFromJavaType(javaType);
        return createDataType(family);
    }

    /**
     * 从 Java 对象创建 DataType
     */
    public JQuickDataType createDataTypeFromValue(Object value) {
        if (value == null) {
            return createDataType(JQuickDataTypeFamily.STRING);
        }

        JQuickDataTypeFamily family = inferFamilyFromValue(value);
        JQuickDataType dataType = createDataType(family);

        // 根据实际值调整长度
        if (family == JQuickDataTypeFamily.STRING && value instanceof String) {
            int length = ((String) value).length();
            if (length > 255) {
                Map<String, Object> params = new HashMap<>();
                params.put("length", Math.min(length, 65535));
                dataType.setParameters(params);
            }
        }

        return dataType;
    }

    /**
     * 获取类型的默认长度
     */
    public int getDefaultLength(JQuickDataTypeFamily family) {
        TypeParameterDefaults defaults = defaultParameters.get(family);
        return defaults != null && defaults.length != null ? defaults.length : 255;
    }

    /**
     * 获取类型的默认精度
     */
    public int getDefaultPrecision(JQuickDataTypeFamily family) {
        TypeParameterDefaults defaults = defaultParameters.get(family);
        return defaults != null && defaults.precision != null ? defaults.precision : 10;
    }

    /**
     * 获取类型的默认小数位
     */
    public int getDefaultScale(JQuickDataTypeFamily family) {
        TypeParameterDefaults defaults = defaultParameters.get(family);
        return defaults != null && defaults.scale != null ? defaults.scale : 0;
    }

    /**
     * 获取所有已注册的数据库类型
     */
    public Set<String> getRegisteredDatabaseTypes() {
        return new HashSet<>(converterMap.keySet());
    }



    /**
     * 类型参数默认值
     */
    public static class TypeParameterDefaults {
        private Integer length;

        private Integer precision;

        private Integer scale;

        public static Builder builder() {
            return new Builder();
        }

        public static class Builder {
            private Integer length;
            private Integer precision;
            private Integer scale;

            public Builder length(int length) {
                this.length = length;
                return this;
            }

            public Builder precision(int precision) {
                this.precision = precision;
                return this;
            }

            public Builder scale(int scale) {
                this.scale = scale;
                return this;
            }

            public TypeParameterDefaults build() {
                TypeParameterDefaults defaults = new TypeParameterDefaults();
                defaults.length = this.length;
                defaults.precision = this.precision;
                defaults.scale = this.scale;
                return defaults;
            }
        }
    }



}
