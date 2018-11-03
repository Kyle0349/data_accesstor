//package com.qhcs.accesser.utils;
//
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.FSDataOutputStream;
//import org.apache.hadoop.fs.FileSystem;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.IOUtils;
//import org.springframework.beans.factory.annotation.Value;
//
//import java.io.InputStream;
//import java.net.URI;
//
//public class HdfsOper {
//
//    @Value("${HDFS_URI}")
//    private String HDFS_URI;
//
//
//    public static String put(InputStream is, String hdfsDir, String scriptName) throws Exception{
//        Configuration conf = new Configuration();
//        URI uri = new URI(HDFS_URI);
//        FileSystem fs = FileSystem.get(uri, conf, "root");
//        if (!fs.exists(new Path(hdfsDir))){
//            fs.mkdirs(new Path(hdfsDir));
//        }
//        FSDataOutputStream fsDataOutputStream = fs.create(new Path(hdfsDir + "/" + scriptName));
//        IOUtils.copyBytes(is, fsDataOutputStream,1024,true);
//        return hdfsDir + "/" + scriptName;
//    }
//
//
//}
