package com.qhcs.accesser.service.impl;

import com.alibaba.fastjson.JSON;
import com.qhcs.accesser.client.HDFSClient;
import com.qhcs.accesser.dao.SourceManagerMapper;
import com.qhcs.accesser.dto.DBInfoDTO;
import com.qhcs.accesser.dto.SourceManagerInfoDTO;
import com.qhcs.accesser.dto.SourceManagerTableDTO;
import com.qhcs.accesser.service.DataAccesser;
import com.qhcs.accesser.utils.ConnManager;
import com.qhcs.accesser.utils.SqoopUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.sql.SQLException;
import java.util.List;

@Service
public class DataAccesserImpl implements DataAccesser {


    @Value("${sqoopTmpDirPrefix}")
    private String sqoopTmpDirPrefix;

    @Value("${scripts_hdfsDir.sqoop}")
    private String scripts_hdfsDir_sqoop;


    @Autowired
    HDFSClient hdfsClient;
    @Autowired
    SourceManagerMapper sourceManagerMapper;


    @Override
    public String configueSqoopJob(String dataSource, boolean isFullDBSync, boolean isAllColumns,
                                    String notSyncColumns, String srcTables, boolean isFullTableSync,
                                    String destOnHdfs, String destDB, String destTablePrefix,
                                    String storeFileType, String keyColumn) throws Exception {

        SourceManagerInfoDTO sourceInfo = getSourceInfoBySourceName(dataSource);
        if (null == sourceInfo) return "该数据源不存在";

        String connection = String.join("",
                sourceInfo.getDb_url_prefix(),
                sourceInfo.getSource_ip(), ":", sourceInfo.getSource_port(), "/", sourceInfo.getSource_db());
        String userName = sourceInfo.getSync_username();
        String password = sourceInfo.getSync_password();
        String sqoopTmpDir = String.join("/",sqoopTmpDirPrefix, destDB);
        String queueName = "default";
        String fieldsSeparator = "\\001";
        String linesSeparator = "\\n";
        String hdfsDir = String.join("/", scripts_hdfsDir_sqoop, sourceInfo.getDb_type_name());
        String scriptName = String.join("_",
                sourceInfo.getSource_name(), sourceInfo.getSync_username());

        if (isFullDBSync){
            //全库全表全量同步
            return "";
        }

        if (isAllColumns && isFullTableSync){
            //全表全量同步
            String scriptStr = SqoopUtils.allColumnsfullTableSyncScript(connection, userName, password, srcTables, destDB,
                    destTablePrefix, sqoopTmpDir, queueName,
                    fieldsSeparator, linesSeparator, hdfsDir, scriptName);

            ByteArrayInputStream is = new ByteArrayInputStream(scriptStr.getBytes());
            return hdfsClient.uploadFile2(is, hdfsDir, scriptName + ".sh");
        }

        if(isAllColumns && !isFullTableSync){
            //全表增量同步
            String scriptStr = SqoopUtils.allColumnsIncremTableSyncScript(connection, userName,
                    password, srcTables, destDB, destTablePrefix, sqoopTmpDir,
                    queueName, fieldsSeparator, linesSeparator, hdfsDir, scriptName, keyColumn);

            ByteArrayInputStream is = new ByteArrayInputStream(scriptStr.getBytes());
            return hdfsClient.uploadFile2(is, hdfsDir, scriptName + ".sh");
        }

        if(!isAllColumns && isFullTableSync){
            //部分列全量

            return "";
        }

        if(!isAllColumns && !isFullTableSync){
            //部分列增量

            return "";
        }

        return "";
    }

    @Override
    public List<String> getDataTypes() {
        return  sourceManagerMapper.getDataTypes();
    }


    @Override
    public String getAllSourceByDBType(String dbType) {
        List<SourceManagerInfoDTO> sourceManagerInfoDTOS =
                sourceManagerMapper.querySourceManagerShowByDbType(dbType);

        return JSON.toJSONString(sourceManagerInfoDTOS);
    }

    @Override
    public String getAllSourcesBySourceName(String sourceName) {
        List<SourceManagerInfoDTO> sourceManagerInfoDTOS =
                sourceManagerMapper.querySourceManagerShowBySourceName(sourceName);
        return JSON.toJSONString(sourceManagerInfoDTOS);
    }

    @Override
    public String addSource(String source_name,
                             String source_type, String source_ip,
                             String source_port, String source_db, String sync_username,
                             String sync_password) throws SQLException, ClassNotFoundException {

        DBInfoDTO dbInfo = getDBInfoByType(source_type);
        if (null == dbInfo) return "不支持该数据库类型";
        boolean isConnection = ConnManager.checkConnection(dbInfo, sync_username, sync_password,
                source_ip,source_port,source_db);
        if (!isConnection) return "连接失败";

        SourceManagerTableDTO sourceManagerTableDTO =
                new SourceManagerTableDTO(
                        source_name,
                        dbInfo.getDb_type_id(),
                        source_ip,
                        source_port,
                        source_db,
                        sync_username,
                        sync_password
                        );

        int rs = sourceManagerMapper.addNewSource(sourceManagerTableDTO);
        if (rs > 0){
            return source_name + " 添加成功";
        }
        return "添加失败";
    }

    @Override
    public String deleteSource(String source_name) {
        int rs = sourceManagerMapper.deleteSource(source_name);
        if (0 == rs){
            return source_name + " 不存在";
        }
        return "删除成功";
    }

    /**
     * 根据数据库类型获取该类型数据库的信息（db_type_id,db_type_name,db_driver,db_url_prefix）
     * @param source_type
     * @return
     * @throws SQLException
     */
    private DBInfoDTO getDBInfoByType(String  source_type) {
        DBInfoDTO dbInfoDTO = sourceManagerMapper.queryDBInfoByDbType(source_type);
        return dbInfoDTO;
    }

    /**
     * 根据数据源名称获取该条数据源的所有信息
     * @param dataSource
     * @return
     * @throws Exception
     */
    private SourceManagerInfoDTO getSourceInfoBySourceName(String dataSource) {
        SourceManagerInfoDTO sourceManagerInfoDTOS =
                sourceManagerMapper.querySourceManagerInfoSourceDbName(dataSource);
        return sourceManagerInfoDTOS;
    }



}
