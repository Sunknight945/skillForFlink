package com.uiys.skillforflink.level3.clickData;

import com.uiys.skillforflink.level3.FileEnum;

public interface FileParseHandler {

    void setRecordVO(RecordVO recordVO);

    RecordVO getRecordVO();

    FileEnum getFileEnum();

    void readFile();


}
