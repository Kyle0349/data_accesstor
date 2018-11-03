package com.qhcs.accesser.oozie;

import com.qhcs.accesser.client.DBClient;
import com.qhcs.accesser.exception.OozieManagerException;
import org.apache.oozie.client.CoordinatorJob;
import org.apache.oozie.client.OozieClient;
import org.apache.oozie.client.OozieClientException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.sql.SQLException;
import java.util.Properties;

/**
 * Created by liqifei
 */

@Component
public class OozieManager {


	@Value("${BASE_OOZIE_URL}")
	private String BASE_OOZIE_URL;
	@Value("${OOZIE_USE_SYSTEM_LIBPATH}")
	private String OOZIE_USE_SYSTEM_LIBPATH;
	@Value("${USER_NAME}")
	private String USER_NAME;
	@Value("${OOZIE_QUEUE_NAME}")
	private String OOZIE_QUEUE_NAME;
	@Value("${JOBTRACKER}")
	private String JOBTRACKER;
	@Value("${NAMENODE}")
	private String NAMENODE;

	@Autowired
	DBClient dbCli;

	@Autowired
	OozieClient ooziecli;

	/**
	 * 提交job 提交一个一次性的job
	 *
	 * @return
	 * @throws OozieClientException
	 */
	public String submitJob(String workflowAppPath) throws OozieManagerException {
		Properties conf = ooziecli.createConfiguration();
		// 配置conf
		conf.setProperty("oozie.use.system.libpath", OOZIE_USE_SYSTEM_LIBPATH);
		conf.setProperty("oozie.wf.application.path", workflowAppPath);
		conf.setProperty("user.name", USER_NAME);
		conf.setProperty("jobTracker", JOBTRACKER);
		conf.setProperty("nameNode", NAMENODE);
		conf.setProperty("queueName", OOZIE_QUEUE_NAME);
		String jobId = null;
		try {
			jobId = ooziecli.submit(conf);
		} catch (OozieClientException e) {
			throw new OozieManagerException("submit job error,error msg is" + e.getMessage());
		}
		return jobId;
	}

	/**
	 * 提交定时任务job
	 * 
	 * @param coordPath
	 * @param workflowAppPath
	 * @param endTime 
	 * @param startTime 
	 * @param frequency 
	 * @return
	 * @throws OozieManagerException
	 */
	public String submitTimerJob(String coordPath, String frequency, String startTime,
								 String endTime, String workflowAppPath) throws OozieManagerException {
		Properties conf = ooziecli.createConfiguration();
		conf.setProperty("oozie.use.system.libpath", OOZIE_USE_SYSTEM_LIBPATH);
		conf.setProperty("oozie.coord.application.path", coordPath);
		conf.setProperty("user.name", USER_NAME);
		conf.setProperty("frequency", frequency);
		conf.setProperty("startTime", startTime);
		conf.setProperty("endTime", endTime);
		conf.setProperty("workflowAppUri", workflowAppPath);
		conf.setProperty("jobTracker", JOBTRACKER);
		conf.setProperty("nameNode", NAMENODE);
		conf.setProperty("queueName", OOZIE_QUEUE_NAME);
		String jobId = null;
		try {
			jobId = ooziecli.submit(conf);
		} catch (OozieClientException e) {
			throw new OozieManagerException("submitTimerJob  error,error msg is" + e.getMessage());
		}
		return jobId;
	}

	/**
	 * 开始运行任务
	 * 
	 * @param jobId
	 * @throws OozieClientException
	 */
	public void runJob(String jobId) throws OozieManagerException {
		try {
			ooziecli.start(jobId);
		} catch (OozieClientException e) {
			throw new OozieManagerException("run job error,error msg is" + e.getMessage());
		}
	}

	/**
	 * 重新跑任务
	 * 
	 * @param jobId
	 * @throws OozieClientException
	 */
	public void reRunJob(String jobId) throws OozieManagerException {
		Properties conf = ooziecli.createConfiguration();
		conf.setProperty(OozieClient.RERUN_FAIL_NODES, "false");
		try {
			ooziecli.reRun(jobId, conf);
		} catch (OozieClientException e) {
			throw new OozieManagerException("reRun job error,error msg is" + e.getMessage());
		}
	}

	/**
	 * 获取任务状态
	 * 
	 * @param jobId
	 * @return
	 * @throws OozieClientException
	 */
	public String getJobStatus(String jobId) throws OozieManagerException {
		try {
			return ooziecli.getStatus(jobId);
		} catch (OozieClientException e) {
			throw new OozieManagerException("getJobStatus error,error msg is" + e.getMessage());
		}
	}

	/**
	 * 判断任务名称是否已经存在
	 * 
	 * @param jobName
	 * @return
	 * @throws OozieManagerException
	 */
	public boolean checkJobName(String jobName) throws OozieManagerException {
		boolean boo = false;
		try {
			String sql = "select count(1) from WF_JOBS where app_name = ?";
			int i = dbCli.select(sql, jobName);
			if (i > 0) {
				boo = true;
			}
		} catch (SQLException e) {
			throw new OozieManagerException("checkJobName error,error msg is" + e.getMessage());
		}
		return boo;
	}

	/**
	 * 查询job的workflow xml 存放的位置
	 * 
	 * @param jobId
	 * @return
	 * @throws OozieManagerException
	 */
	public String selectAppUri(String jobId) throws OozieManagerException {
		String workflowAppUri = null;
		String sql = "select app_path from WF_JOBS where id = ?";
		try {
			workflowAppUri = dbCli.selectAppUri(sql, jobId);
		} catch (SQLException e) {
			throw new OozieManagerException("select AppUri error error msg is " + e.getMessage());
		}
		if (workflowAppUri == null) {
			throw new OozieManagerException("Query information of less than job according to jobId!");
		}
		return workflowAppUri;
	}


	/**
	 * 杀死job
	 * @param jobId
	 * @return
	 * @throws OozieManagerException
	 */
	public String killJob(String jobId) throws OozieManagerException {
		try {
			CoordinatorJob coordJobInfo = ooziecli.getCoordJobInfo(jobId);
			String user = coordJobInfo.getUser();
			System.out.println("user: " + user);
			ooziecli.kill(jobId);
		} catch (OozieClientException e) {
			e.printStackTrace();
		}
		return "job: " + jobId + "is killed";

	}


}
