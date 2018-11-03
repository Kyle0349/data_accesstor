package com.qhcs.accesser.service.impl;

import com.qhcs.accesser.exception.DispatchServiceException;
import com.qhcs.accesser.oozie.OozieManager;
import com.qhcs.accesser.service.DispatchService;
import com.qhcs.accesser.utils.XmlCreateUtils;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

/**
 * 
 * @author liqifei
 */
@Service
public class DispatchServiceImpl implements DispatchService {


	@Value("${OOZIE_WF_APPLICATION_PATH}")
	private String OOZIE_WF_APPLICATION_PATH;
	@Value("${OOZIE_COORDINATOR_APPLICATION_PATH}")
	private String OOZIE_COORDINATOR_APPLICATION_PATH;
	@Value("${END_TIME}")
	private String END_TIME;

	@Autowired
	private OozieManager manager;

	public String submitJob(String jobName, String shPath, String... args) throws DispatchServiceException {
		if (StringUtils.isBlank(jobName) || StringUtils.isBlank(shPath)) {
			throw new DispatchServiceException("parameters can not be empty！");
		}
		// 判断jobName是否已经存在
		boolean boo = checkJobName(jobName);
		if (boo) {
			throw new DispatchServiceException("jobName has already existed!");
		}
		// 创建xml并上传到指定的路径
		String workflowAppPath = XmlCreateUtils.createShellWorkflowXml(OOZIE_WF_APPLICATION_PATH, jobName,
				shPath, args);
		// 调用oozie接口提交任务
		//return manager.submitJob(workflowAppPath, JOBTRACKER, NAMENODE);
		return manager.submitJob(workflowAppPath);
	}

	public String submitTimerJob(String jobName, String shPath, String frequency, String startTime, String... args)
			throws DispatchServiceException {
		if (StringUtils.isBlank(jobName) || StringUtils.isBlank(shPath) || StringUtils.isBlank(frequency)
				|| StringUtils.isBlank(startTime)) {
			throw new DispatchServiceException("parameters can not be empty！");
		}
		// 判断jobName是否已经存在
		boolean boo = checkJobName(jobName);
		if (boo) {
			throw new DispatchServiceException("jobName has already existed!");
		}
		// 创建workflowXml并上传到指定的路径
		String workflowAppPath = XmlCreateUtils.createShellWorkflowXml(OOZIE_WF_APPLICATION_PATH, jobName,
				shPath, args);
		// 创建xml并上传到指定的路径
		String coordinatorAppPath = XmlCreateUtils.createShellCoordinatorXml(
				OOZIE_COORDINATOR_APPLICATION_PATH, jobName);
		return manager.submitTimerJob(coordinatorAppPath, frequency, startTime, END_TIME, workflowAppPath);
	}


	private boolean checkJobName(String jobName) throws DispatchServiceException {
		if (StringUtils.isBlank(jobName)) {
			throw new DispatchServiceException("parameter can not be empty！");
		}
		return manager.checkJobName(jobName);
	}

	public void runJob(String jobId) throws DispatchServiceException {
		if (StringUtils.isBlank(jobId)) {
			throw new DispatchServiceException("parameter can not be empty！");
		}
		manager.runJob(jobId);
	}

	public void reRunJob(String jobId) throws DispatchServiceException {
		if (StringUtils.isBlank(jobId)) {
			throw new DispatchServiceException("parameter can not be empty！");
		}
		manager.reRunJob(jobId);
	}

	@Override
	public String killJob(String jobId) throws DispatchServiceException {
		if (StringUtils.isBlank(jobId)) {
			throw new DispatchServiceException("parameter can not be empty！");
		}
		String s = manager.killJob(jobId);
		return s;
	}
}
