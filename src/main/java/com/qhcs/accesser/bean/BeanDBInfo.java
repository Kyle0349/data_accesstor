package com.qhcs.accesser.bean;

public class BeanDBInfo {

    private int db_type_id;
    private String db_type_name;
    private String db_driver;
    private String db_url_prefix;


    public int getDb_type_id() {
        return db_type_id;
    }

    public void setDb_type_id(int db_type_id) {
        this.db_type_id = db_type_id;
    }

    public String getDb_type_name() {
        return db_type_name;
    }

    public void setDb_type_name(String db_type_name) {
        this.db_type_name = db_type_name;
    }

    public String getDb_driver() {
        return db_driver;
    }

    public void setDb_driver(String db_driver) {
        this.db_driver = db_driver;
    }

    public String getDb_url_prefix() {
        return db_url_prefix;
    }

    public void setDb_url_prefix(String db_url_prefix) {
        this.db_url_prefix = db_url_prefix;
    }
}
