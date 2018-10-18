package com.qhcs.accesser.service.impl;

import com.qhcs.accesser.client.HDFSClient;
import com.qhcs.accesser.service.HdfsService;
import org.springframework.stereotype.Service;

@Service
public class HdfsServiceImpl implements HdfsService {
	
	HDFSClient hdfsCli = new HDFSClient();

	public long getCatalogSize(String catalogPath) {
		return hdfsCli.getCatalogSize(catalogPath);
	}

	public void uploadFile(String diskPath, String hdfsPath, String hdfsFileName) {
		hdfsCli.insertDatas(diskPath, hdfsPath, hdfsFileName);
	}

}
