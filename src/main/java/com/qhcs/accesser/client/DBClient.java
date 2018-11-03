package com.qhcs.accesser.client;

import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

@Component
public class DBClient {

	private static final Logger logger = LoggerFactory.getLogger(HiveClient.class);


	private ComboPooledDataSource oozieSource = new ComboPooledDataSource("oozieSource");
	
	public Connection getOozieConnection() {
		Connection conn = null;
		try {
			conn = oozieSource.getConnection();
		} catch (Exception e) {
			logger.error("DBClient.getHiveConnection error ,error msg is :", e);
		}
		
		return conn;
	}

	public PreparedStatement createStatement(Connection conn, String sql) {
		PreparedStatement ps = null;
		try {
			ps = conn.prepareStatement(sql);
		} catch (SQLException e) {
			logger.error("DBClient.createStatement error ,error msg is :", e);
		}
		return ps;
	}

	public void close(Connection conn, PreparedStatement ps, ResultSet rs) {

		if (null != rs) {
			try {
				rs.close();
			} catch (SQLException e) {
				logger.error("DBClient.close ResultSet error, error msg is:", e);
			}
		}

		if (null != ps) {
			try {
				ps.close();
			} catch (SQLException e) {
				logger.error("DBClient.close PreparedStatement error ,error msg is:", e);
			}
		}

		if (null != conn) {
			try {
				conn.close();
			} catch (SQLException e) {
				logger.error("DBClient.close Connection error ,error msg is:", e);
			}
		}
	}
	
	public int select(String sql,String tableName) throws SQLException {
		int count = 0;
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = getOozieConnection();
			ps = createStatement(conn, sql);
			ps.setString(1, tableName);
			rs = ps.executeQuery();
			while (rs.next()) {
				count = rs.getInt(1);
			}
		} finally {
			close(conn, ps, rs);
		}

		return count;
	}
	/**
	 * 查询job的workflow xml 存放的位置
	 * @param sql
	 * @param jobId
	 * @return
	 * @throws SQLException
	 */
	public String selectAppUri(String sql,String jobId) throws SQLException{
		String workflowAppUri = null;
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = getOozieConnection();
			ps = createStatement(conn, sql);
			ps.setString(1, jobId);
			rs = ps.executeQuery();
			while (rs.next()) {
				workflowAppUri = rs.getString(1);
			}
		} finally {
			close(conn, ps, rs);
		}

		return workflowAppUri;
	}
}
