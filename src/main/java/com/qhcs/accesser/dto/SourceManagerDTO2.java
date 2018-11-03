package com.qhcs.accesser.dto;

import javax.persistence.*;

@Table( name = "test_tb_source_manager" )
public class SourceManagerDTO2 {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY, generator = "JDBC")
    private String sourceId;
    private String sourceName;
    private String sourceTypeId;
    private String sourceIp;
    private String sourcePort;
    private String sourceDb;
    @Transient
    private String dbDriver;
    private String syncUsername;
    private String syncPassword;
    @Transient
    private String dbUrlPrefix;


    public String getSourceId() {
        return sourceId;
    }

    public void setSourceId(String sourceId) {
        this.sourceId = sourceId;
    }

    public String getSourceName() {
        return sourceName;
    }

    public void setSourceName(String sourceName) {
        this.sourceName = sourceName;
    }

    public String getSourceTypeId() {
        return sourceTypeId;
    }

    public void setSourceTypeId(String sourceTypeId) {
        this.sourceTypeId = sourceTypeId;
    }

    public String getSourceIp() {
        return sourceIp;
    }

    public void setSourceIp(String sourceIp) {
        this.sourceIp = sourceIp;
    }

    public String getSourcePort() {
        return sourcePort;
    }

    public void setSourcePort(String sourcePort) {
        this.sourcePort = sourcePort;
    }

    public String getSourceDb() {
        return sourceDb;
    }

    public void setSourceDb(String sourceDb) {
        this.sourceDb = sourceDb;
    }

    public String getDbDriver() {
        return dbDriver;
    }

    public void setDbDriver(String dbDriver) {
        this.dbDriver = dbDriver;
    }

    public String getSyncUsername() {
        return syncUsername;
    }

    public void setSyncUsername(String syncUsername) {
        this.syncUsername = syncUsername;
    }

    public String getSyncPassword() {
        return syncPassword;
    }

    public void setSyncPassword(String syncPassword) {
        this.syncPassword = syncPassword;
    }

    public String getDbUrlPrefix() {
        return dbUrlPrefix;
    }

    public void setDbUrlPrefix(String dbUrlPrefix) {
        this.dbUrlPrefix = dbUrlPrefix;
    }
}
