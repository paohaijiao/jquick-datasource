package com.github.paohaijiao.connector;

import lombok.Data;

import java.util.HashMap;

@Data
public class JQuickDataSourceConnector {

    private String driverClass;

    private String url;

    private String host;

    private String port;

    private String username;

    private String password;

    private String schema;

    private HashMap<String,Object> extract=new HashMap<>();


    public JQuickDataSourceConnector() {}

    public JQuickDataSourceConnector(String driverClass, String url, String username, String password) {
        this.driverClass = driverClass;
        this.url = url;
        this.username = username;
        this.password = password;
    }
    public JQuickDataSourceConnector(String driverClass, String url, String username, String password,String schema) {
        this.driverClass = driverClass;
        this.url = url;
        this.username = username;
        this.password = password;
        this.schema = schema;
    }
    public JQuickDataSourceConnector(String driverClass, String url, String username, String password,String schema,HashMap<String,Object>  attr) {
        this.driverClass = driverClass;
        this.url = url;
        this.username = username;
        this.password = password;
        this.schema = schema;
        this.extract.putAll(attr);
    }

    public String getByKeyStr(String key) {
        if(null==this.extract||null==key){
            return null;
        }
        if(!this.extract.containsKey(key)){
            return null;
        }
        return this.extract.get(key).toString();

    }
    public void setByKey(String key, Object value) {
        if(null==this.extract||null==value){
            return;
        }
        this.extract.put(key, value);
    }
}
