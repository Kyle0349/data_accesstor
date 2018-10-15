package com.qhcs.accesser.controller;


import com.qhcs.accesser.bean.BeanDBSourceShow;
import com.qhcs.accesser.service.impl.DataAccesserImpl;
import org.apache.commons.lang.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import javax.servlet.http.HttpServletRequest;
import java.sql.SQLException;
import java.util.List;

@Controller
@RequestMapping("/dataAccesser")
public class DataAccesserController {

    @Autowired
    private DataAccesserImpl dataAccesser;


    @RequestMapping("/configueSqoopJob")
    @ResponseBody
    public String configueSqoopJob(HttpServletRequest request) throws Exception{

        String dataSource = request.getParameter("dataSource");
        boolean isFullDBSync = Boolean.parseBoolean(request.getParameter("isFullDBSync"));
        boolean isAllColumns = Boolean.parseBoolean(request.getParameter("isAllColumns"));
        String notSyncColumns = request.getParameter("notSyncColumns");
        String srcTables = request.getParameter("srcTables");
        boolean isFullTables = Boolean.parseBoolean(request.getParameter("isFullTables"));
        String destOnHdfs = request.getParameter("destOnHdfs"); // hive
        String destDB = request.getParameter("destDB"); // "cbd"
        String destTablePrefix = request.getParameter("destTablePrefix"); //"sqoop_test";
        String storeFileType = request.getParameter("storeFileType"); // "parquet";
        String keyColumn = request.getParameter("keyColumn"); //"";

        if (!StringUtils.isNotBlank(dataSource)) return "dataSource 数据源不能为空";
        if (!isAllColumns && !StringUtils.isNotBlank(notSyncColumns)) return "非全表同步，需要提供不同步的列notSyncColumns";
        if (!StringUtils.isNotBlank(srcTables)) return "srcTables 同步的表不能为空";
        if (!StringUtils.isNotBlank(destOnHdfs)) {
            destOnHdfs = "hive";
        }
        if (!StringUtils.isNotBlank(destDB)) return "destDB 目标数据库不能为空";
        if (!StringUtils.isNotBlank(destTablePrefix)) return "destTablePrefix 目标表的前缀不能为空";
        if (!StringUtils.isNotBlank(storeFileType)) {
            storeFileType = "parquet";
        }
        if (!isFullTables && !StringUtils.isNotBlank(keyColumn)) return "增量同步， keyColumn 需要提供增量字段【日期】";

        boolean configResult = dataAccesser.configueSqoopJob(dataSource, isFullDBSync, isAllColumns,
                notSyncColumns, srcTables, isFullTables, destOnHdfs,
                destDB, destTablePrefix, storeFileType, keyColumn);
        if (configResult) return "同步脚本创建成功";
        return "同步脚本创建失败";

    }

    @RequestMapping("/getAllSourcesByDBType")
    @ResponseBody
    public List<BeanDBSourceShow> getAllSourcesByDBType(HttpServletRequest request) throws Exception{
        String dbType = request.getParameter("dbType");
        List<BeanDBSourceShow> allSource = dataAccesser.getAllSourceByDBType(dbType);
        return allSource;
    }


    @RequestMapping("/getAllSourcesBySourceName")
    @ResponseBody
    public BeanDBSourceShow getAllSourcesBySourceName(HttpServletRequest request) throws Exception{
        String sourceName = request.getParameter("sourceName");
        BeanDBSourceShow beanDBSourceShow = dataAccesser.getAllSourcesBySourceName(sourceName);
        return beanDBSourceShow;
    }


    @RequestMapping("/getDatabaseType")
    @ResponseBody
    public List<String> getDatabaseType() throws SQLException{
        List<String> dataTypes = dataAccesser.getDataTypes();
        return dataTypes;
    }


    @RequestMapping("/addSource")
    @ResponseBody
    public String addSource(HttpServletRequest request)
            throws SQLException, ClassNotFoundException {

        String source_name = request.getParameter("source_name");
        String source_type = request.getParameter("source_type");
        String source_ip = request.getParameter("source_ip");
        String source_port = request.getParameter("source_port");
        String source_db = request.getParameter("source_db");
        String sync_username = request.getParameter("sync_username");
        String sync_password = request.getParameter("sync_password");

        if (!StringUtils.isNotBlank(source_name)) return "source_name 不能为空";
        if (!StringUtils.isNotBlank(source_type)) return "source_type 不能为空";
        if (!StringUtils.isNotBlank(source_ip)) return "source_ip 不能为空";
        if (!StringUtils.isNotBlank(source_port)) return "source_port 不能为空";
        if (!StringUtils.isNotBlank(source_db)) return "source_db 不能为空";
        if (!StringUtils.isNotBlank(sync_username)) return "sync_username 不能为空";
        if (!StringUtils.isNotBlank(sync_password)) return "sync_password 不能为空";

        String addResult = dataAccesser.addSource(source_name, source_type, source_ip,
                source_port, source_db, sync_username, sync_password);

        return addResult;
    }

}
