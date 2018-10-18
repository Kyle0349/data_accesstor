package com.qhcs.accesser.config;

public class SystemConfig_bak {
	/******* hive *******/
	public static final String HIVE_QUEUE_NAME = getProperties("HIVE_QUEUE_NAME");// yarn队列名
	public static final String ENGINE_TYPE = getProperties("ENGINE_TYPE");// hive 计算引擎类型 (0表示Tez，1表示MapReduce)
	/******* hdfs *******/
	public static final String HDFS_URI = getProperties("HDFS_URI");
	public static final String HDFS_OPER_USER = getProperties("HDFS_OPER_USER");
	/******* oozie *******/
	public static final String OOZIE_QUEUE_NAME = getProperties("OOZIE_QUEUE_NAME");
	public static final String USER_NAME = getProperties("USER_NAME");
	public static final String BASE_OOZIE_URL = getProperties("BASE_OOZIE_URL");
	public static final String OOZIE_USE_SYSTEM_LIBPATH = getProperties("OOZIE_USE_SYSTEM_LIBPATH");
	public static final String NAMENODE = getProperties("NAMENODE");
	public static final String JOBTRACKER = getProperties("JOBTRACKER");
	public static final String OOZIE_WF_APPLICATION_PATH = getProperties("OOZIE_WF_APPLICATION_PATH");
	public static final String START_TIME = getProperties("START_TIME");
	public static final String END_TIME = getProperties("END_TIME");
	public static final String OOZIE_COORDINATOR_APPLICATION_PATH = getProperties("OOZIE_COORDINATOR_APPLICATION_PATH");
	/**
	 * 获取配置文件信息
	 * @param key
	 * @return
	 */
	private static String getProperties(String key) {
		String value = null;
		ConfigReader reader = ConfigReader.read("core");
		try {
			value = reader.getConstant(key);
		} catch (Exception e) {
			e.printStackTrace();
		}
		return value;
	}
}
