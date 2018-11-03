package com.qhcs.accesser.dto;

public class SourceManagerShowDTO {

    private String source_id;
    private String source_name;
    private String db_type_name;
    private String source_ip;
    private String source_port;
    private String source_db;

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

    public String getDb_type_name() {
        return db_type_name;
    }

    public void setDb_type_name(String db_type_name) {
        this.db_type_name = db_type_name;
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
}
