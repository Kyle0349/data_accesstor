package com.qhcs.accesser.utils;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import com.qhcs.accesser.bean.BeanDBInfo;

import java.sql.*;

public class ConnManager {


    public static Connection getMysqlConnection() throws SQLException {

        ComboPooledDataSource mysqlSource = new ComboPooledDataSource("mysqlSource");
        return mysqlSource.getConnection();

    }


    public static boolean checkConnection(BeanDBInfo dbInfo, String username, String password,
                                          String source_ip, String source_port, String dbName)
            throws ClassNotFoundException {
        String conn = String.join("",
                dbInfo.getDb_url_prefix(),source_ip,":",source_port,"/",dbName);
        Class.forName(dbInfo.getDb_driver());
        Connection connection = null;
        try {
            connection = DriverManager.getConnection(conn, username, password);
        } catch (SQLException e) {
            e.printStackTrace();
            return false;
        }
        ConnManager.close(connection, null, null);
        return true;
    }



    public static void close(Connection conn, Statement stmt, ResultSet rs) {
        if (null != rs){
            try {
                rs.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (null != stmt){
            try {
                stmt.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }

        if (null != conn){
            try {
                conn.close();
            } catch (SQLException e) {
                e.printStackTrace();
            }
        }



    }


}
