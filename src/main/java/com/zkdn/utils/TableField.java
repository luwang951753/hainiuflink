package com.zkdn.utils;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

/**
 * Created with IntelliJ IDEA.
 *
 * @Auther: lw
 * @Date: 2022-03-03-2:59 下午
 * @Description:
 */
public class TableField {
    public static final String AVRO_TABLE_TYPE = "avro";
    public static final String PARQUET_TABLE_TYPE = "parquet";
    public static final String SERDE = "serde";
    public static final String INPUTFORMAT = "inputformat";
    public static final String OUTPUTFORMAT = "oututformat";

    //不同类型表的java属性类型与hive字段类型映射关系
    private static Map<String, Map<String, String>> javaType2HiveTypeMap = new HashMap<>();
    //不同类型的serde、inputformat、outputformat映射关系
    private static Map<String, Map<String, String>> formatMap = new HashMap<>();

    static {
        HashMap<String, String> avro2HiveMap = new HashMap<>();
        avro2HiveMap.put(String.class.getName(), "string");
        avro2HiveMap.put(Integer.class.getName(), "int");
        avro2HiveMap.put(Double.class.getName(), "double");
        avro2HiveMap.put(Short.class.getName(), "int");
        avro2HiveMap.put(Byte.class.getName(), "int");
        avro2HiveMap.put(Long.class.getName(), "bigint");
        avro2HiveMap.put(Float.class.getName(), "float");
        avro2HiveMap.put(Boolean.class.getName(), "boolean");
        avro2HiveMap.put(Date.class.getName(), "bigint");
        avro2HiveMap.put(Map.class.getName(), "map<string, string>");
        avro2HiveMap.put(char.class.getName(), "String");
        javaType2HiveTypeMap.put(AVRO_TABLE_TYPE, avro2HiveMap);

        HashMap<String, String> parquet2HiveMap = new HashMap<>();
        parquet2HiveMap.put(String.class.getName(), "string");
        parquet2HiveMap.put(Integer.class.getName(), "bigint");
        parquet2HiveMap.put(Double.class.getName(), "double");
        parquet2HiveMap.put(Short.class.getName(), "bigint");
        parquet2HiveMap.put(Byte.class.getName(), "bigint");
        parquet2HiveMap.put(Long.class.getName(), "bigint");
        parquet2HiveMap.put(Float.class.getName(), "float");
        parquet2HiveMap.put(Boolean.class.getName(), "boolean");
        parquet2HiveMap.put(Date.class.getName(), "bigint");
        parquet2HiveMap.put(Map.class.getName(), "map<string, string>");
        parquet2HiveMap.put(char.class.getName(), "String");
        javaType2HiveTypeMap.put(PARQUET_TABLE_TYPE, parquet2HiveMap);


        HashMap<String, String> avroFormatMap = new HashMap<>();
        avroFormatMap.put(SERDE, "ROW FORMAT SERDE 'org.apache.hadoop.hive.serde2.avro.AvroSerDe'\n");
        avroFormatMap.put(INPUTFORMAT, "STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat'\n");
        avroFormatMap.put(OUTPUTFORMAT, "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'\n");
        formatMap.put(AVRO_TABLE_TYPE, avroFormatMap);

        HashMap<String, String> parquetFormatMap = new HashMap<>();
        parquetFormatMap.put(SERDE, "ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'\n");
        parquetFormatMap.put(INPUTFORMAT, "STORED AS INPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat'\n");
        parquetFormatMap.put(OUTPUTFORMAT, "OUTPUTFORMAT 'org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat'\n");
        formatMap.put(PARQUET_TABLE_TYPE, parquetFormatMap);


    }

    //字段名称
    private String name;
    //java类型
    private String javaType;
    //字段默认值
    private Object defaultValue;
    //表的类型
    private String tableType;

    public TableField(String name, String javaType, Object defaultValue, String tableType) {
        this.name = name;
        this.javaType = javaType;
        this.defaultValue = defaultValue;
        this.tableType = tableType;
    }

    public TableField(String name, Object defaultValue, String tableType) {
        this.name = name;
        this.defaultValue = defaultValue;
        this.tableType = tableType;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getJavaType() {
        return javaType;
    }

    public void setJavaType(String javaType) {
        this.javaType = javaType;
    }

    public Object getDefaultValue() {
        return defaultValue;
    }

    public void setDefaultValue(Object defaultValue) {
        this.defaultValue = defaultValue;
    }

    //根据字段类型映射出hive字段类型
    public String getHiveType(){
        return javaType2HiveTypeMap.get(tableType).get(this.javaType);
    }

    public static String getTableFormatStr(String tableType){
        StringBuilder sb = new StringBuilder();
        Map<String, String> stringStringMap = formatMap.get(tableType);
        sb.append(stringStringMap.get(SERDE))
                .append(stringStringMap.get(INPUTFORMAT))
                .append(stringStringMap.get(OUTPUTFORMAT));
        return sb.toString();
    }

    public String getTableType() {
        return tableType;
    }

    public void setTableType(String tableType) {
        this.tableType = tableType;
    }
}
