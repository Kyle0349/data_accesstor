package com.qhcs.accesser.utils;

import com.qhcs.accesser.Application;
import com.qhcs.accesser.client.HDFSClient;
import com.qhcs.accesser.exception.XmlCreateException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.w3c.dom.Document;
import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.File;
import java.util.UUID;

/**
 * 创建xml的工具类
 * 
 * @author liqifei
 * @DATA 2018年10月9日
 */

public class XmlCreateUtils {

	private static final Logger logger = LoggerFactory.getLogger(XmlCreateUtils.class);

	/**
	 * 生成一个不带参数的执行shell脚本的workflow.xml文件
	 * 
	 * @param xmlFilePath 需要将生成的xml写到哪里，例如："/app/shell/jobName"（hdfs路径）
	 * @param jobName     表示创建job的名称
	 * @param shPath      表示要执行的.sh所在的hdfs上的地址
	 * @param args        表示要执行.sh文件需要的参数（如果不需要参数则传null）
	 * @throws
	 */
	public static String createShellWorkflowXml(String xmlFilePath, String jobName, String shPath, String... args) throws XmlCreateException {
		String xmlName = jobName + "_workflow.xml";
		try {
			// 创建DocumentBuilderFactory
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			// 创建Docume  
			DocumentBuilder builder = factory.newDocumentBuilder();
			// 创建Document
			Document document = builder.newDocument();

			document.setXmlStandalone(true);

			// 创建根节点，并添加属性
			Element bookstore = document.createElement("workflow-app");
			bookstore.setAttribute("name", jobName);
			bookstore.setAttribute("xmlns", "uri:oozie:workflow:0.5");
			document.appendChild(bookstore);
			// 为根节点添加子节点 start
			Element startEle = document.createElement("start");
			startEle.setAttribute("to", "shell-start");
			bookstore.appendChild(startEle);

			// 为根节点添加子节点 kill
			Element killEle = document.createElement("kill");
			killEle.setAttribute("name", "kill");
			bookstore.appendChild(killEle);

			// 为kill节点添加子节点 message
			Element messageEle = document.createElement("message");
			messageEle.setTextContent("操作失败，错误消息[${wf:errorMessage(wf:lastErrorNode())}]");
			killEle.appendChild(messageEle);

			// 为根节点添加子节点 action
			Element actionEle = document.createElement("action");
			actionEle.setAttribute("name", "shell-start");
			bookstore.appendChild(actionEle);

			// 为action节点添加子节点 shell
			Element shellEle = document.createElement("shell");
			shellEle.setAttribute("xmlns", "uri:oozie:shell-action:0.2");
			actionEle.appendChild(shellEle);

			// 为shell节点添加子节点 jobtracker
			Element jobTrackerEle = document.createElement("job-tracker");
			jobTrackerEle.setTextContent("${jobTracker}");
			shellEle.appendChild(jobTrackerEle);

			// 为shell节点添加子节点 name-node
			Element nameNodeEle = document.createElement("name-node");
			nameNodeEle.setTextContent("${nameNode}");
			shellEle.appendChild(nameNodeEle);

			// 为shell节点添加子节点 configuration
			Element configurationEle = document.createElement("configuration");
			shellEle.appendChild(configurationEle);

			// 为configuration节点添加子节点 property
			Element propertyEle = document.createElement("property");
			configurationEle.appendChild(propertyEle);

			// 为property节点添加子节点 name
			Element nameEle = document.createElement("name");
			nameEle.setTextContent("mapred.job.queue.name");
			propertyEle.appendChild(nameEle);

			// 为property节点添加子节点 value
			Element valueEle = document.createElement("value");
			valueEle.setTextContent("${queueName}");
			propertyEle.appendChild(valueEle);

			// 为shell节点添加子节点exec
			String shName = shPath.substring(shPath.lastIndexOf("/") + 1, shPath.length());
			Element execEle = document.createElement("exec");
			execEle.setTextContent(shName);
			shellEle.appendChild(execEle);

			// 为shell节点添加子节点argument
			if (args != null && args.length > 0) {
				for (String arg : args) {
					Element argumentEle = document.createElement("argument");
					argumentEle.setTextContent(arg);
					shellEle.appendChild(argumentEle);
				}
			}

			// 为shell节点添加子节点file
			Element fileEle = document.createElement("file");
			if (shPath.startsWith("/")) {
				fileEle.setTextContent("${nameNode}" + shPath + "#" + shName);
			} else {
				fileEle.setTextContent("${nameNode}" + "/" + shPath + "#" + shName);
			}
			shellEle.appendChild(fileEle);
			// 为action节点添加子节点ok
			Element okEle = document.createElement("ok");
			okEle.setAttribute("to", "end");
			actionEle.appendChild(okEle);

			// 为action节点添加子节点error
			Element errorEle = document.createElement("error");
			errorEle.setAttribute("to", "kill");
			actionEle.appendChild(errorEle);

			// 为根节点添加子节点 end
			Element endEle = document.createElement("end");
			endEle.setAttribute("name", "end");
			bookstore.appendChild(endEle);

			// 创建TransformerFactory对象
			TransformerFactory tff = TransformerFactory.newInstance();

			// 创建Transformer对象
			Transformer tf = tff.newTransformer();

			// 设置输出数据时换行
			tf.setOutputProperty(OutputKeys.INDENT, "yes");

			// 使用Transformer的transform()方法将DOM树转换成XML
			File xmlFile = new File(xmlName);
			tf.transform(new DOMSource(document), new StreamResult(xmlFile));
			// 将产生在本地的xml上传到hdfs上
			HDFSClient client = Application.applicationContext.getBean(HDFSClient.class);
			boolean boo = client.insertDatas(xmlFile.getAbsolutePath(), xmlFilePath, xmlName);
			// 将本地的xml文件删除
			xmlFile.delete();
			if (!boo) {
				throw new XmlCreateException("生成的xml文件上传到hdfs失败！");
			}
		} catch (Exception e) {
			logger.error("createShellWorkflowXml function error error msg is " + e.getMessage());
			throw new XmlCreateException(e.getMessage());
		}
		return xmlFilePath + "/" + xmlName;
	}

	/**
	 * 生成定时任务的xml文件,并上传到hdfs上
	 * 
	 * @param coorXmlPath
	 * @param jobName
	 * @throws XmlCreateException
	 * @return 返回coorXml在hdfs上的路径
	 */
	public static String createShellCoordinatorXml(String coorXmlPath, String jobName) throws XmlCreateException {
		String xmlName = jobName + "_coordinator.xml";
		try {
			// 创建DocumentBuilderFactory
			DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
			// 创建DocumentBuilder
			DocumentBuilder builder = factory.newDocumentBuilder();
			// 创建Document
			Document document = builder.newDocument();

			document.setXmlStandalone(true);

			// 创建根节点，并添加属性
			Element bookstore = document.createElement("coordinator-app");
			bookstore.setAttribute("name", UUID.randomUUID().toString());
			bookstore.setAttribute("frequency", "${frequency}");
			bookstore.setAttribute("start", "${startTime}");
			bookstore.setAttribute("end", "${endTime}");
			bookstore.setAttribute("timezone", "UTC");
			bookstore.setAttribute("xmlns", "uri:oozie:coordinator:0.2");
			document.appendChild(bookstore);

			// 为根节点创建action子节点
			Element actionEle = document.createElement("action");
			bookstore.appendChild(actionEle);

			// 为action节点添加workflow子节点
			Element workflowEle = document.createElement("workflow");
			actionEle.appendChild(workflowEle);

			// 为workflow节点添加子节点 app-path
			Element appPathEle = document.createElement("app-path");
			appPathEle.setTextContent("${workflowAppUri}");
			workflowEle.appendChild(appPathEle);

			// 为workflow添加configuration 子节点
			Element configurationEle = document.createElement("configuration");
			workflowEle.appendChild(configurationEle);

			// 为configuration添加property 子节点
			Element propertyEle01 = document.createElement("property");
			configurationEle.appendChild(propertyEle01);

			Element nameEle01 = document.createElement("name");
			nameEle01.setTextContent("jobTracker");
			propertyEle01.appendChild(nameEle01);

			Element valueEle01 = document.createElement("value");
			valueEle01.setTextContent("${jobTracker}");
			propertyEle01.appendChild(valueEle01);

			// 为configuration添加property 子节点
			Element propertyEle02 = document.createElement("property");
			configurationEle.appendChild(propertyEle02);

			Element nameEle02 = document.createElement("name");
			nameEle02.setTextContent("nameNode");
			propertyEle02.appendChild(nameEle02);

			Element valueEle02 = document.createElement("value");
			valueEle02.setTextContent("${nameNode}");
			propertyEle02.appendChild(valueEle02);

			// 为configuration添加property 子节点
			Element propertyEle03 = document.createElement("property");
			configurationEle.appendChild(propertyEle03);

			Element nameEle03 = document.createElement("name");
			nameEle03.setTextContent("queueName");
			propertyEle03.appendChild(nameEle03);

			Element valueEle03 = document.createElement("value");
			valueEle03.setTextContent("${queueName}");
			propertyEle03.appendChild(valueEle03);

			// 创建TransformerFactory对象
			TransformerFactory tff = TransformerFactory.newInstance();

			// 创建Transformer对象
			Transformer tf = tff.newTransformer();

			// 设置输出数据时换行
			tf.setOutputProperty(OutputKeys.INDENT, "yes");

			// 使用Transformer的transform()方法将DOM树转换成XML
			File xmlFile = new File(xmlName);
			tf.transform(new DOMSource(document), new StreamResult(xmlFile));
			// 将产生在本地的xml上传到hdfs上
			HDFSClient client = Application.applicationContext.getBean(HDFSClient.class);
			boolean boo = client.insertDatas(xmlFile.getAbsolutePath(), coorXmlPath, xmlName);
			// 将本地的xml文件删除
			xmlFile.delete();
			if (!boo) {
				throw new XmlCreateException("生成的xml文件上传到hdfs失败！");
			}
		} catch (Exception e) {
			throw new XmlCreateException(e.getMessage());
		}
		return coorXmlPath + "/" + xmlName;
	}
}
