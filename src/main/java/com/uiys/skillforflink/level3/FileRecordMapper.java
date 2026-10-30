package com.uiys.skillforflink.level3;


import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;

@DS("mysql")
@Mapper
public interface FileRecordMapper extends BaseMapper<FileRecord> {
}
