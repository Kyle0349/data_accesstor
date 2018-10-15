package com.qhcs.accesser.utils;

import java.io.ByteArrayInputStream;

public class SqoopUtils {

    /**
     * 生成全表全量导数据的sqoop脚本
     * @param connection
     * @param userName
     * @param password
     * @param srcTablesStr
     * @param hiveDB
     * @param destTablePrefix
     * @param sqoopTmpDir
     * @param queueName
     * @param fieldsSeparator
     * @param linesSeparator
     * @param hdfsDir
     * @param scriptName
     * @throws Exception
     */
    public static void allColumnsfullTableSyncScript( String connection, String userName, String password,
                                           String srcTablesStr, String hiveDB, String destTablePrefix,
                                           String sqoopTmpDir, String queueName, String fieldsSeparator,
                                           String linesSeparator, String hdfsDir, String scriptName) throws Exception{

        String[] srcTables = srcTablesStr.split(",");
        StringBuilder sb = new StringBuilder();
        sb.append("#!/bin/sh").append("\n\n");
        sb.append("# parameters").append("\n");
        sb.append(String.join("","queueName='",queueName,"'")).append("\n");
        sb.append(String.join("","connection='",connection,"'")).append("\n");
        sb.append(String.join("","userName='",userName,"'")).append("\n");
        sb.append(String.join("","password='",password,"'")).append("\n");
        sb.append(String.join("","hiveDB='",hiveDB,"'")).append("\n");
        sb.append(String.join("","destTablePrefix='",destTablePrefix,"'")).append("\n");
        sb.append(String.join("","sqoopTmpDir='",sqoopTmpDir,"'")).append("\n");
        sb.append(String.join("","fieldsSeparator='",fieldsSeparator,"'")).append("\n");
        sb.append(String.join("","linesSeparator='",linesSeparator,"'")).append("\n");
        sb.append("condition=' 1=1 '").append("\n\n");
        sb.append("# srcTables").append("\n");
        sb.append("srcTables='").append("\n");
        for (String srcTable : srcTables) {
            sb.append(srcTable).append("\n");
        }
        sb.append("'").append("\n\n\n");
        sb.append("#begin to import data into hivedb").append("\n");
        sb.append("for tableName in ${srcTables}").append("\n");
        sb.append("do").append("\n\n");
        sb.append("sqoop import ");
        sb.append("-D mapred.job.queue.name=");
        sb.append("${queueName} ");
        sb.append("-D mapred.job.name=");
        sb.append("sqoop_import_${hiveDB}_${destTablePrefix}_${tableName}").append(" \\").append("\n");
        sb.append("--connect ${connection}").append(" \\").append("\n");
        sb.append("--username ${userName}").append(" \\").append("\n");
        sb.append("--password ${password}").append(" \\").append("\n");
        sb.append("--query \"select * from ${tableName} where ${condition} and \\$CONDITIONS \"").append(" \\").append("\n");
        sb.append("--hive-database ${hiveDB}").append(" \\").append("\n");
        sb.append("--hive-table ${destTablePrefix}_${tableName}").append(" \\").append("\n");
        sb.append("--hive-import").append(" \\").append("\n");
        sb.append("--hive-overwrite").append(" \\").append("\n");
        sb.append("--target-dir ${sqoopTmpDir}").append(" \\").append("\n");
        sb.append("--delete-target-dir").append(" \\").append("\n");
        sb.append("--hive-drop-import-delims").append(" \\").append("\n");
        sb.append("--fields-terminated-by ${fieldsSeparator}").append(" \\").append("\n");
        sb.append("--lines-terminated-by '\\n'").append(" \\").append("\n");
        sb.append("--null-string '\\\\N'").append(" \\").append("\n");
        sb.append("--null-non-string '\\\\N'").append(" \\").append("\n");
        sb.append("--as-parquetfile").append(" \\").append("\n");
        sb.append("-m 1").append("\n\n");
        sb.append("done").append("\n");

        ByteArrayInputStream is = new ByteArrayInputStream(sb.toString().getBytes());
        HdfsOper.put(is, hdfsDir, scriptName + ".sh");

    }


    /**
     * 生成增量同步数据sqoop脚本
     * @param connection
     * @param userName
     * @param password
     * @param srcTablesStr
     * @param hiveDB
     * @param destTablePrefix
     * @param sqoopTmpDir
     * @param queueName
     * @param fieldsSeparator
     * @param linesSeparator
     * @param hdfsDir
     * @param scriptName
     */

    public static void allColumnsIncremTableSyncScript(String connection, String userName, String password,
                                                String srcTablesStr, String hiveDB, String destTablePrefix,
                                                String sqoopTmpDir, String queueName, String fieldsSeparator,
                                                String linesSeparator, String hdfsDir, String scriptName,
                                                String keyColumn) throws Exception {

        String[] srcTables = srcTablesStr.split(",");
        StringBuilder sb = new StringBuilder();
        sb.append("#!/bin/sh").append("\n\n");
        sb.append("# parameters").append("\n");
        sb.append(String.join("","queueName='",queueName,"'")).append("\n");
        sb.append(String.join("","connection='",connection,"'")).append("\n");
        sb.append(String.join("","userName='",userName,"'")).append("\n");
        sb.append(String.join("","password='",password,"'")).append("\n");
        sb.append(String.join("","hiveDB='",hiveDB,"'")).append("\n");
        sb.append(String.join("","destTablePrefix='",destTablePrefix,"'")).append("\n");
        sb.append(String.join("","sqoopTmpDir='",sqoopTmpDir,"'")).append("\n");
        sb.append(String.join("","fieldsSeparator='",fieldsSeparator,"'")).append("\n");
        sb.append(String.join("","linesSeparator='",linesSeparator,"'")).append("\n");
        sb.append(String.join("","keyColumn='",keyColumn,"'")).append("\n\n");
        sb.append("endPoint=`date +%Y-%m-%d`").append("\n\n");
        sb.append("# srcTables").append("\n");
        sb.append("srcTables='").append("\n");
        for (String srcTable : srcTables) {
            sb.append(srcTable).append("\n");
        }
        sb.append("'").append("\n\n\n");

        sb.append("#begin to import data into hivedb").append("\n");
        sb.append("for tableName in ${srcTables}").append("\n");
        sb.append("do").append("\n\n");

        sb.append("# get the start point").append("\n");
        sb.append("startFrom=`hive -e \"").append("\n");
        sb.append("select max(${keyColumn}) from ${hiveDB}.${destTablePrefix}_${tableName};").append("\n");
        sb.append("\"'").append("\n");
        sb.append("condition=\" ${keyColumn} >= ${startFrom} and ${keyColumn} < ${endPoint} \"").append("\n");

        sb.append("sqoop import ");
        sb.append("-D mapred.job.queue.name=");
        sb.append("${queueName} ");
        sb.append("-D mapred.job.name=");
        sb.append("sqoop_import_${hiveDB}_${destTablePrefix}_${tableName}").append(" \\").append("\n");
        sb.append("--connect ${connection}").append(" \\").append("\n");
        sb.append("--username ${userName}").append(" \\").append("\n");
        sb.append("--password ${password}").append(" \\").append("\n");
        sb.append("--query \"select * from ${tableName} where ${condition} and \\$CONDITIONS \"").append(" \\").append("\n");
        sb.append("--hive-database ${hiveDB}").append(" \\").append("\n");
        sb.append("--hive-table ${destTablePrefix}_${tableName}").append(" \\").append("\n");
        sb.append("--hive-import").append(" \\").append("\n");
        sb.append("--target-dir ${sqoopTmpDir}").append(" \\").append("\n");
        sb.append("--delete-target-dir").append(" \\").append("\n");
        sb.append("--hive-drop-import-delims").append(" \\").append("\n");
        sb.append("--fields-terminated-by ${fieldsSeparator}").append(" \\").append("\n");
        sb.append("--lines-terminated-by '\\n'").append(" \\").append("\n");
        sb.append("--null-string '\\\\N'").append(" \\").append("\n");
        sb.append("--null-non-string '\\\\N'").append(" \\").append("\n");
        sb.append("--as-parquetfile").append(" \\").append("\n");
        sb.append("-m 1").append("\n\n");

        sb.append("done").append("\n");


        ByteArrayInputStream is = new ByteArrayInputStream(sb.toString().getBytes());
        HdfsOper.put(is, hdfsDir, scriptName + "_zl.sh");

    }






}
