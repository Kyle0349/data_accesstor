package com.qhcs.accesser.client;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.mchange.v2.c3p0.ComboPooledDataSource;
import org.apache.commons.lang3.tuple.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

@Component
public class HiveClient {

	@Value("${ENGINE_TYPE}")
	private static String ENGINE_TYPE;
	@Value("${HIVE_QUEUE_NAME}")
	private static String HIVE_QUEUE_NAME;


	private static final Logger logger = LoggerFactory.getLogger(HiveClient.class);
	private ComboPooledDataSource hiveSource = new ComboPooledDataSource("hiveSource");

	private static String setQueue;
	static {
		if ("0".equals(ENGINE_TYPE)) {
			setQueue = "set tez.queue.name=" + HIVE_QUEUE_NAME;
		}
		if ("1".equals(ENGINE_TYPE)) {
			setQueue = "set mapreduce.job.queuename=" + HIVE_QUEUE_NAME;
		}
	}

	public Connection getConnection() {
		Connection conn = null;
		try {
			conn = hiveSource.getConnection();
		} catch (Exception e) {
			logger.error("getHiveConnection error ,error msg is :", e);
		}
		return conn;
	}

	public PreparedStatement createStatement(Connection conn, String sql) {
		PreparedStatement ps = null;
		try {
			ps = conn.prepareStatement(sql);
		} catch (SQLException e) {
			logger.error("createStatement error ,error msg is :", e);
		}
		return ps;
	}

	public void close(Connection conn, PreparedStatement ps, ResultSet rs) {

		if (null != rs) {
			try {
				rs.close();
			} catch (SQLException e) {
				logger.error("close ResultSet error, error msg is:", e);
			}
		}

		if (null != ps) {
			try {
				ps.close();
			} catch (SQLException e) {
				logger.error("close PreparedStatement error ,error msg is:", e);
			}
		}

		if (null != conn) {
			try {
				conn.close();
			} catch (SQLException e) {
				logger.error("close Connection error ,error msg is:", e);
			}
		}
	}

	/**
	 * 获取表的行数
	 * 
	 * @param tableName 输入表名的时候要加上表所在的库名
	 * @return
	 */
	public long getCount(String tableName) throws SQLException {
		long count = 0;
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		try {
			conn = getConnection();
			String sql = String.join(" ", "select count(1) from", tableName);
			ps = createStatement(conn, sql);
			ps.execute(setQueue);
			rs = ps.executeQuery();
			while (rs.next()) {
				count = rs.getLong(1);
			}
		} finally {
			close(conn, ps, rs);
		}

		return count;
	}

	/**
	 * 获取表每个字段的字段饱和度
	 * 
	 * @param tableName
	 * @return map的key 表示字段名，value表示字段对应的非空值的个数
	 */
	public Map<String, Long> getFieldSaturation(String tableName) throws SQLException {
		// 获取表的字段信息
		List<String> fields = listFields(tableName);
		Map<String, Long> map = Maps.newHashMap();
		for (String field : fields) {
			Pair<String, Long> pair = getFieldSaturation(tableName, field);
			map.put(pair.getKey(), pair.getValue());
		}
		return map;
	}
	/**
	 * 查询所有数据库的名称
	 * @return
	 * @throws SQLException
	 */
	public List<String> listDatabases() throws SQLException {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		List<String> databases = Lists.newArrayList();
		try {
			conn = getConnection();
			String sql = "show databases";
			ps = createStatement(conn, sql);
			rs = ps.executeQuery();
			while (rs.next()) {
				String databaseName = rs.getString(1);
				databases.add(databaseName);
			}
		} finally {
			close(conn, ps, rs);
		}
		return databases;
	}
	
	public List<String> listTables(String databaseName) throws SQLException {
		Connection conn = null;
		PreparedStatement ps = null; 
		ResultSet rs = null;
		List<String> tables = Lists.newArrayList();
		try {
			conn = getConnection();
			String sql = "show tables";
			ps = createStatement(conn, sql);
			ps.execute("use "+databaseName);
			rs = ps.executeQuery();
			while (rs.next()) {
				String table = rs.getString(1);
				tables.add(table);
			}
		} finally {
			close(conn, ps, rs);
		}
		return tables;
	}

	/**
	 * 获取表的列信息
	 * 
	 * @param tableName
	 * @return
	 */
	public List<String> listFields(String tableName) throws SQLException {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		List<String> fields = Lists.newArrayList();
		try {
			conn = getConnection();
			String sql = String.join(" ", "desc", tableName);
			ps = createStatement(conn, sql);
			rs = ps.executeQuery();
			while (rs.next()) {
				String fieldName = rs.getString(1);
				fields.add(fieldName);
			}
		} finally {
			close(conn, ps, rs);
		}
		return fields;
	}

	private Pair<String, Long> getFieldSaturation(String tableName, String field) throws SQLException {
		Connection conn = null;
		PreparedStatement ps = null;
		ResultSet rs = null;
		Long saturation = null;
		try {
			conn = getConnection();
			String sql = String.join(" ", "select count(1) from", tableName, "where", field, "is not null");
			ps = createStatement(conn, sql);
			ps.execute(setQueue);
			rs = ps.executeQuery();
			if (rs.next()) {
				saturation = rs.getLong(1);
			}
		} finally {
			close(conn, ps, rs);
		}
		return Pair.of(field, saturation);
	}
}
