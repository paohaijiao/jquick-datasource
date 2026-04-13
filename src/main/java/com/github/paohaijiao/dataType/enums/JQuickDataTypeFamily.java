package com.github.paohaijiao.dataType.enums;

public enum JQuickDataTypeFamily {
    /**
     * 可变长度字符串，对应 Java String
     */
    STRING,

    /**
     * 固定长度字符串
     */
    CHAR,

    /**
     * 长文本，对应 Java String（大文本）
     */
    TEXT,

    /**
     * 字符大对象，对应 Java String（超大文本）
     */
    CLOB,


    /**
     * 32位整数，对应 Java Integer / int
     */
    INTEGER,

    /**
     * 64位整数，对应 Java Long / long
     */
    LONG,

    /**
     * 16位整数，对应 Java Short / short
     */
    SHORT,

    /**
     * 8位整数，对应 Java Byte / byte
     */
    BYTE,


    /**
     * 单精度浮点数，对应 Java Float / float
     */
    FLOAT,

    /**
     * 双精度浮点数，对应 Java Double / double
     */
    DOUBLE,

    /**
     * 精确小数，对应 Java BigDecimal
     */
    DECIMAL,


    /**
     * 日期（年月日），对应 Java LocalDate
     */
    DATE,

    /**
     * 时间（时分秒），对应 Java LocalTime
     */
    TIME,

    /**
     * 时间戳（年月日时分秒毫秒），对应 Java LocalDateTime / Instant
     */
    TIMESTAMP,


    /**
     * 二进制大对象，对应 Java byte[]
     */
    BLOB,

    /**
     * 固定长度二进制，对应 Java byte[]
     */
    BINARY,

    /**
     * 可变长度二进制，对应 Java byte[]
     */
    VARBINARY,


    /**
     * 布尔值，对应 Java Boolean / boolean
     */
    BOOLEAN,


    /**
     * JSON 类型，对应 Java JsonNode / Map / List
     */
    JSON,


    /**
     * 其他/自定义类型
     */
    OTHER
}
