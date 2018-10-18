package com.qhcs.accesser.service;

public interface HdfsService {
	
	long getCatalogSize(String catalogPath);
	
	void uploadFile(String diskPath, String hdfsPath, String hdfsFileName);

}
