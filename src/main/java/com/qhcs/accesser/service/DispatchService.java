package com.qhcs.accesser.service;

import com.qhcs.accesser.exception.DispatchServiceException;

/**
 * 
 * @author liqifei
 * @DATA 2018年3月12日
 */
public interface DispatchService {
	/**
	 * 提交job任务到oozie
	 * 
	 * @param jobName
	 *            job名称
	 * @param shPath
	 *            要执行的sh文件在hdfs上的路径
	 * @param args
	 *            执行sh文件需要的外部参数
	 * @return 返回生成的jobId
	 * @throws
	 */
	String submitJob(String jobName, String shPath, String... args) throws DispatchServiceException;
	/**
	 * 提交定时任务job到oozie
	 * @param jobName job 的名称
	 * @param shPath   要执行的sh文件在hdfs上的路径
	 * @param frequency  设置的定时时间
	 * @param startTime  任务开始时间
	 * @param args 执行sh文件需要的外部参数
	 * @return
	 * @throws DispatchServiceException
	 */
	String submitTimerJob(String jobName, String shPath, String frequency, String startTime, String... args)
			throws DispatchServiceException;

	/**
	 * 运行job任务（立即运行）
	 * 
	 * @param jobId
	 *            是jobId而非jobName
	 * @throws DispatchServiceException
	 */
	void runJob(String jobId) throws DispatchServiceException;

	/**
	 * 重新运行job任务
	 * 
	 * @param jobId
	 * @throws DispatchServiceException
	 */
	void reRunJob(String jobId) throws DispatchServiceException;
	
	


}
