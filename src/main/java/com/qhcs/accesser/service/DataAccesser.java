package com.qhcs.accesser.service;

import java.sql.SQLException;
import java.util.List;

public interface DataAccesser {

    public String configueSqoopJob(String dataSource, boolean isFullDBSync, boolean isAllColumns,
                                    String notSyncColumns, String srcTables, boolean isFullTableSync,
                                    String destOnHdfs, String destDB, String destTablePrefix,
                                    String storeFileType, String keyColumn) throws Exception;

    public List<String> getDataTypes() throws SQLException;

    public String getAllSourceByDBType(String dbType) throws Exception;


    public String getAllSourcesBySourceName(String sourceName) throws Exception;



    public String addSource(String source_name, String source_type, String source_ip,
                             String source_port, String source_db, String sync_username,
                             String sync_password) throws Exception;


    public String deleteSource(String source_name) throws Exception;



}
