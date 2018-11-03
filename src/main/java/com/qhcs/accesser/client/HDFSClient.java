package com.qhcs.accesser.client;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.*;
import java.net.URI;

@Component
public class HDFSClient {

	private static final Logger logger = LoggerFactory.getLogger(HDFSClient.class);

	@Value("${HDFS_URI}")
	private String HDFS_URI;
	@Value("${HDFS_OPER_USER}")
	private String HDFS_OPER_USER;

	/**
	 * 加载fileSystem
	 */
	private FileSystem getFileSystem() {
		FileSystem fileSystem = null;
		try {
			Configuration conf = new Configuration();
			URI uri = new URI(HDFS_URI);
			fileSystem = FileSystem.get(uri, conf, HDFS_OPER_USER);
		} catch (Exception e) {
			logger.error("HDFSClient.getFileSystem error ,error msg is", e);
		}
		return fileSystem;
	}

	/**
	 * 关闭fileSystem
	 *
	 * @param fileSystem
	 */
	public void close(FileSystem fileSystem) {
		try {
			if (null != fileSystem) {
				fileSystem.close();
			}
		} catch (IOException e) {
			logger.error("close fileSystem error", e);
		}
	}

	/**
	 * 获取目录大小 单位为 byte
	 * 
	 * @param catalogPath 目录路径
	 * @return
	 */
	public long getCatalogSize(String catalogPath) {
		FileSystem fileSystem = this.getFileSystem();
		long size = 0;
		try {
			size = fileSystem.getContentSummary(new Path(catalogPath)).getLength();
		} catch (Exception e) {
			logger.error("getCatalogSize error, error msg is ", e);
		}
		return size;
	}

	/**
	 * 上传文件
	 *
	 * @param fileName
	 * @param srcPath
	 * @return
	 */
	public boolean uploadFile(String fileName, String srcPath, String dstPath) {

		FileSystem fileSystem = this.getFileSystem();
		try {
			if (srcPath.endsWith("/")) {
				fileSystem.copyFromLocalFile(new Path(srcPath + fileName), new Path(dstPath));
			} else {
				srcPath += "/";
				fileSystem.copyFromLocalFile(new Path(srcPath + fileName), new Path(dstPath));
			}
			return true;
		} catch (IOException e) {
			logger.error("HDFS 上传文件失败", e);
		} finally {
			this.close(fileSystem);
		}
		return false;
	}


	/**
	 *
	 * @param is
	 * @param hdfsDir
	 * @param scriptName
	 * @return
	 * @throws Exception
	 */
	public String uploadFile2(InputStream is, String hdfsDir, String scriptName) throws Exception{
		Configuration conf = new Configuration();
		URI uri = new URI(HDFS_URI);
		FileSystem fs = FileSystem.get(uri, conf, "root");
		if (!fs.exists(new Path(hdfsDir))){
			fs.mkdirs(new Path(hdfsDir));
		}
		FSDataOutputStream fsDataOutputStream = fs.create(new Path(hdfsDir + "/" + scriptName));
		IOUtils.copyBytes(is, fsDataOutputStream,1024,true);
		return hdfsDir + "/" + scriptName;

	}

	/**
	 * 删除文件
	 *
	 * @param fileName
	 * @return
	 * @paramfilePath
	 */
	public boolean deleteFile(String fileName, String srcPath) {

		FileSystem fileSystem = this.getFileSystem();
		try {
			if (!srcPath.endsWith("/")) {
				srcPath += "/";
			}
			fileSystem.deleteOnExit(new Path(srcPath + fileName));
			return true;
		} catch (IOException e) {
			logger.error("删除文件失败", e);
			return false;
		} finally {
			this.close(fileSystem);
		}
	}

	/**
	 * 将本地文件复制到hdfs对应的目录文件中
	 * 
	 * @param diskPath
	 * @param hdfsPath
	 * @param hdfsFileName
	 * @return
	 */
	public boolean insertDatas(String diskPath, String hdfsPath, String hdfsFileName) {
		boolean boo = true;
		FileSystem fileSystem = null;
		InputStream in = null;
		OutputStream out = null;
		try {
			fileSystem = this.getFileSystem();
			Path path = new Path(hdfsPath);
			Path path02 = new Path(hdfsPath + "/" + hdfsFileName);
			if (!fileSystem.exists(path)) {// 如果不存在则创建
				fileSystem.mkdirs(path);
			}
			if (!fileSystem.exists(path02)) {
				fileSystem.createNewFile(path02);
			}
			// }
			// 拷贝本地文件到hdfs上
			in = new BufferedInputStream(new FileInputStream(diskPath));
			out = fileSystem.create(path02);
			IOUtils.copyBytes(in, out, 4096, true);
		} catch (Exception e) {
			logger.error("HDFSClient.insertDatas is error,error msg is", e);
			boo = false;
		} finally {
			try {
				if (in != null) {
					in.close();
				}
				if (out != null) {
					out.close();
				}
			} catch (Exception e2) {
				logger.error("close stream error,error msg is", e2);
			}
			this.close(fileSystem);
		}
		return boo;
	}


}
