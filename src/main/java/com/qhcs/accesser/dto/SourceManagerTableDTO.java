package com.qhcs.accesser.dto;

public class SourceManagerTableDTO {

    private String source_name;
    private int source_type_id;
    private String source_ip;
    private String source_port;
    private String source_db;
    private String sync_username;
    private String sync_password;

    public SourceManagerTableDTO() {
    }

    public SourceManagerTableDTO(String source_name,
                                 int source_type_id, String source_ip,
                                 String source_port, String source_db,
                                 String sync_username, String sync_password) {
        this.source_name = source_name;
        this.source_type_id = source_type_id;
        this.source_ip = source_ip;
        this.source_port = source_port;
        this.source_db = source_db;
        this.sync_username = sync_username;
        this.sync_password = sync_password;
    }

    public String getSource_name() {
        return source_name;
    }

    public void setSource_name(String source_name) {
        this.source_name = source_name;
    }

    public int getSource_type_id() {
        return source_type_id;
    }

    public void setSource_type_id(int source_type_id) {
        this.source_type_id = source_type_id;
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
