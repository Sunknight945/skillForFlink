package com.uiys.skillforflink.level3.clickData;

import com.baomidou.dynamic.datasource.annotation.DS;
import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;

import java.util.List;


@DS("ck")
@Mapper
public interface OneTypeCkEntityMapper extends BaseMapper<OneTypeCkEntity> {

    List<String> selectExistMd5s(@Param("md5List") List<String> md5s);

    Integer batchInsert(@Param("list") List<OneTypeCkEntity> list);


}
