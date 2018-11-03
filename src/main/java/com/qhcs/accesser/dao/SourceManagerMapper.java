package com.qhcs.accesser.dao;


import com.qhcs.accesser.dto.DBInfoDTO;
import com.qhcs.accesser.dto.SourceManagerInfoDTO;
import com.qhcs.accesser.dto.SourceManagerTableDTO;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;

@Mapper
public interface SourceManagerMapper {

    public SourceManagerInfoDTO querySourceManagerInfoSourceDbName(@Param("sourceName") String dbName);

    public List<SourceManagerInfoDTO> querySourceManagerShowByDbType(@Param("dbType") String dbType);

    public List<SourceManagerInfoDTO> querySourceManagerShowBySourceName(@Param("sourceName") String dbName);

    public DBInfoDTO queryDBInfoByDbType(@Param("dbType") String dbType);

    public int addNewSource(SourceManagerTableDTO sourceManagerTableDTO);

    public List<String> getDataTypes();

    public int deleteSource(@Param("source_name") String source_name);


}
