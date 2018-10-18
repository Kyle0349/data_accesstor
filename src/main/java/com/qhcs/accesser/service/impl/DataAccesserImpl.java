package com.qhcs.accesser.service.impl;

import com.qhcs.accesser.bean.BeanDBInfo;
import com.qhcs.accesser.bean.BeanDBSourceInfo;
import com.qhcs.accesser.bean.BeanDBSourceShow;
import com.qhcs.accesser.service.DataAccesser;
import com.qhcs.accesser.utils.ConnManager;
import com.qhcs.accesser.utils.SqoopUtils;
import com.qhcs.accesser.utils.XmlCreateUtils;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

@Service
public class DataAccesserImpl implements DataAccesser {


    @Value("${sqoopTmpDirPrefix}")
    private String sqoopTmpDirPrefix;


    @Value("${scripts_hdfsDir.sqoop}")
    private String scripts_hdfsDir_sqoop;


    @Override
    public String configueSqoopJob(String dataSource, boolean isFullDBSync, boolean isAllColumns,
                                    String notSyncColumns, String srcTables, boolean isFullTableSync,
                                    String destOnHdfs, String destDB, String destTablePrefix,
                                    String storeFileType, String keyColumn) throws Exception {

        BeanDBSourceInfo sourceInfo = getSourceInfoBySourceName(dataSource);
        if (null == sourceInfo) return "该数据源不存在";

        String connection = String.join("",
                sourceInfo.getDb_url_prefix(),
                sourceInfo.getSource_ip(), ":", sourceInfo.getSource_port(), "/", sourceInfo.getSource_db());
        String userName = sourceInfo.getSync_username();
        String password = sourceInfo.getSync_password();
        String sqoopTmpDir = String.join("/",sqoopTmpDirPrefix, destDB);
        String queueName = "default";
        String fieldsSeparator = "\\001";
        String linesSeparator = "\\n";
        String hdfsDir = String.join("/", scripts_hdfsDir_sqoop, sourceInfo.getSource_type_name());
        String scriptName = String.join("_",
                sourceInfo.getSource_name(), sourceInfo.getSync_username());

        if (isFullDBSync){
            //全库全表全量同步
            return "";
        }

        if (isAllColumns && isFullTableSync){
            //全表全量同步
            String scriptPath = SqoopUtils.allColumnsfullTableSyncScript(connection, userName, password, srcTables, destDB,
                    destTablePrefix, sqoopTmpDir, queueName,
                    fieldsSeparator, linesSeparator, hdfsDir, scriptName);
            return scriptPath;
        }

        if(isAllColumns && !isFullTableSync){
            //全表增量同步
            String scriptPath = SqoopUtils.allColumnsIncremTableSyncScript(connection, userName,
                    password, srcTables, destDB, destTablePrefix, sqoopTmpDir,
                    queueName, fieldsSeparator, linesSeparator, hdfsDir, scriptName, keyColumn);
            return scriptPath;
        }

        if(!isAllColumns && isFullTableSync){
            //部分列全量

            return "";
        }

        if(!isAllColumns && !isFullTableSync){
            //部分列增量

            return "";
        }

        return "";
    }

    @Override
    public List<String> getDataTypes() throws SQLException {
        Connection conn = ConnManager.getMysqlConnection();
        Statement stmt = conn.createStatement();
        ResultSet resultSet = stmt.executeQuery("select db_type_name from test_tb_db_type");
        List<String> dbTypes = new ArrayList<>();
        while (resultSet.next()){
            dbTypes.add(resultSet.getString(1));
        }
        ConnManager.close(conn, stmt, resultSet);
        if(dbTypes.size()>0) return dbTypes;
        return null;
    }

    @Override
    public List<BeanDBSourceShow> getAllSourceByDBType(String dbType) throws Exception {
        Connection conn = ConnManager.getMysqlConnection();
        Statement stmt = conn.createStatement();
        String sql = "select a.source_id, a.source_name, b.db_type_name," +
                " a.source_ip, a.source_port, a.source_db, b.db_driver, a.sync_username, a.sync_password " +
                " from test_tb_source_manager a " +
                " left join test_tb_db_type b on a.source_type_id = b.db_type_id " +
                " where b.db_type_name ='" + dbType + "'";
        ResultSet resultSet = stmt.executeQuery(sql);
        List<BeanDBSourceShow> sourceBeans = new ArrayList<>();
        while (resultSet.next()){
            BeanDBSourceShow beanDBSourceShow = pariseResultSet1(resultSet);
            sourceBeans.add(beanDBSourceShow);
        }
        ConnManager.close(conn, stmt,resultSet);
        if (sourceBeans.size()>0) return sourceBeans;
        return null;
    }

    @Override
    public BeanDBSourceShow getAllSourcesBySourceName(String sourceName) throws Exception {
        Connection conn = ConnManager.getMysqlConnection();
        Statement stmt = conn.createStatement();
        String sql = "select a.source_id, a.source_name, b.db_type_name," +
                " a.source_ip, a.source_port, a.source_db, b.db_driver, a.sync_username, a.sync_password " +
                " from test_tb_source_manager a " +
                " left join test_tb_db_type b on a.source_type_id = b.db_type_id " +
                " where a.source_name ='" + sourceName + "'";
        ResultSet resultSet = stmt.executeQuery(sql);
        BeanDBSourceShow beanDBSourceShow = null;
        while (resultSet.next()){
            beanDBSourceShow = pariseResultSet1(resultSet);
        }
        return beanDBSourceShow;
    }

    @Override
    public String addSource(String source_name,
                             String source_type, String source_ip,
                             String source_port, String source_db, String sync_username,
                             String sync_password) throws SQLException, ClassNotFoundException {

        BeanDBInfo dbInfo = getDBInfoByName(source_type);
        if (null == dbInfo) return "不支持该数据库类型";
        boolean isConnection = ConnManager.checkConnection(dbInfo, sync_username, sync_password,
                source_ip,source_port,source_db);
        if (!isConnection) return "连接失败";

        Connection mysqlConnection = ConnManager.getMysqlConnection();
        Statement stmt = mysqlConnection.createStatement();
        String sql = "insert into test_tb_source_manager(source_name, source_type_id, source_ip, " +
                "source_port, source_db, sync_username, sync_password) values('" + source_name + "'," +
                dbInfo.getDb_type_id() + ", '" + source_ip + "', '" + source_port + "', '" + source_db + "', '" +
                sync_username + "', '" + sync_password + "')";

        stmt.execute(sql);
        ConnManager.close(mysqlConnection, stmt, null);
        return "添加数据源成功";
    }


    @Override
    public void submit2Oozie(String scriptPath) throws Exception {
        XmlCreateUtils.createShellWorkflowXml("",
                "job_sync_sqoop_000001",scriptPath,null);
    }


    /**
     * 根据数据库类型获取数据库类型id
     * @param source_type
     * @return
     * @throws SQLException
     */
    private BeanDBInfo getDBInfoByName(String  source_type) throws SQLException {
        Connection mysqlConnection = ConnManager.getMysqlConnection();
        Statement stmt = mysqlConnection.createStatement();
        ResultSet resultSet = stmt
                .executeQuery("select db_type_id," +
                        "db_type_name, db_driver, db_url_prefix" +
                        " from test_tb_db_type where db_type_name = '" + source_type + "'");
        BeanDBInfo beanDBInfo = null;
        while (resultSet.next()){
            beanDBInfo = new BeanDBInfo();
            beanDBInfo.setDb_type_id(resultSet.getInt(1));
            beanDBInfo.setDb_type_name(resultSet.getString(2));
            beanDBInfo.setDb_driver(resultSet.getString(3));
            beanDBInfo.setDb_url_prefix(resultSet.getString(4));
        }
        return beanDBInfo;
    }


    /**
     * 根据数据源名称获取该数据源的所有信息
     * @param dataSource
     * @return
     * @throws Exception
     */
    private BeanDBSourceInfo getSourceInfoBySourceName(String dataSource) throws Exception{
        Connection mysqlConnection = ConnManager.getMysqlConnection();
        Statement stmt = mysqlConnection.createStatement();

        String sql = "select a.source_id, a.source_name, b.db_type_name," +
                " a.source_ip, a.source_port, a.source_db, b.db_driver, " +
                "a.sync_username, a.sync_password, b.db_url_prefix " +
                " from test_tb_source_manager a " +
                " left join test_tb_db_type b on a.source_type_id = b.db_type_id " +
                "where a.source_name = '" + dataSource + "'";

        ResultSet resultSet = stmt.executeQuery(sql);
        BeanDBSourceInfo beanDBSourceInfo = null;
        while (resultSet.next()){
            beanDBSourceInfo = pariseResultSet(resultSet);
        }
        ConnManager.close(mysqlConnection, stmt, resultSet);
        return beanDBSourceInfo;
    }

    /**
     * 解析一条数据源的所有信息
     * @param rs
     * @return
     * @throws SQLException
     */
    private BeanDBSourceInfo pariseResultSet(ResultSet rs) throws SQLException{
        BeanDBSourceInfo beanDBSourceInfo = new BeanDBSourceInfo();
        beanDBSourceInfo.setSource_id(rs.getString(1));
        beanDBSourceInfo.setSource_name(rs.getString(2));
        beanDBSourceInfo.setSource_type_name(rs.getString(3));
        beanDBSourceInfo.setSource_ip(rs.getString(4));
        beanDBSourceInfo.setSource_port(rs.getString(5));
        beanDBSourceInfo.setSource_db(rs.getString(6));
        beanDBSourceInfo.setDb_driver(rs.getString(7));
        beanDBSourceInfo.setSync_username(rs.getString(8));
        beanDBSourceInfo.setSync_password(rs.getString(9));
        beanDBSourceInfo.setDb_url_prefix(rs.getString(10));
        return beanDBSourceInfo;
    }

    /**
     * 解析数据源的信息2
     * @param rs
     * @return
     * @throws SQLException
     */
    private BeanDBSourceShow pariseResultSet1(ResultSet rs) throws SQLException{
        BeanDBSourceShow beanDBSourceShow = new BeanDBSourceShow();
        beanDBSourceShow.setSource_id(rs.getString(1));
        beanDBSourceShow.setSource_name(rs.getString(2));
        beanDBSourceShow.setSource_type_name(rs.getString(3));
        beanDBSourceShow.setSource_ip(rs.getString(4));
        beanDBSourceShow.setSource_port(rs.getString(5));
        beanDBSourceShow.setSource_db(rs.getString(6));
        return beanDBSourceShow;
    }

}
