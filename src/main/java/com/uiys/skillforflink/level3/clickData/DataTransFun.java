package com.uiys.skillforflink.level3.clickData;

import com.alibaba.fastjson2.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;


@Slf4j
public class DataTransFun implements MapFunction<String, RecordVO> {
    @Override
    public RecordVO map(String msg) throws Exception {
        JSONObject payload = JSONObject.parseObject(msg).getJSONObject("payload");
        boolean insertFlag = payload.get("op").equals("c");
        if (insertFlag) {
            JSONObject after = (JSONObject) payload.get("after");
            RecordVO recordVO = new RecordVO(after.get("file_name").toString(), after.get("file_path").toString());
            log.info(recordVO.toString());
            return recordVO;
        }
        return null;
    }
}
