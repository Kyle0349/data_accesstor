package com.qhcs.accesser.bean;

public class BeanDBSourceInfo {

    private String source_id;
    private String source_name;
    private String source_type_name;
    private String source_ip;
    private String source_port;
    private String source_db;
    private String db_driver;
    private String sync_username;
    private String sync_password;
    private String db_url_prefix;

    public String getDb_url_prefix() {
        return db_url_prefix;
    }

    public void setDb_url_prefix(String db_url_prefix) {
        this.db_url_prefix = db_url_prefix;
    }

    public String getSource_id() {
        return source_id;
    }

    public void setSource_id(String source_id) {
        this.source_id = source_id;
    }

    public String getSource_name() {
        return source_name;
    }

    public void setSource_name(String source_name) {
        this.source_name = source_name;
    }

    public String getSource_type_name() {
        return source_type_name;
    }

    public void setSource_type_name(String source_type_name) {
        this.source_type_name = source_type_name;
    }

    public String getSource_ip() {
        return source_ip;
    }

    public void setSource_ip(String source_ip) {
        this.source_ip = source_ip;
    }

    public String getSource_port() {
        return source_port;
    }

    public void setSource_port(String source_port) {
        this.source_port = source_port;
    }

    public String getSource_db() {
        return source_db;
    }

    public void setSource_db(String source_db) {
        this.source_db = source_db;
    }

    public String getDb_driver() {
        return db_driver;
    }

    public void setDb_driver(String db_driver) {
        this.db_driver = db_driver;
    }

    public String getSync_username() {
        return sync_username;
    }

    public void setSync_username(String sync_username) {
        this.sync_username = sync_username;
    }

    public String getSync_password() {
        return sync_password;
    }

    public void setSync_password(String sync_password) {
        this.sync_password = sync_password;
    }
}
