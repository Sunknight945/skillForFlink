package com.uiys.skillforflink.level3.clickData;

import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.OpenContext;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.springframework.context.ApplicationContext;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

@Slf4j
@Data
@Component
@EqualsAndHashCode(callSuper = true)
public class FileParseHandlerSelect extends RichMapFunction<RecordVO, FileParseHandler> {


    private List<FileParseHandler> fileParseHandlers;

    @Override
    public void open(OpenContext openContext) throws Exception {
        log.info("过来了吗?");
        super.open(openContext);
        ApplicationContext applicationContext = SpringContextHolder.getApplicationContext();
        this.fileParseHandlers = new ArrayList<>(applicationContext.getBeansOfType(FileParseHandler.class).values());
        log.info("设置了吗?");
    }

    @Override
    public FileParseHandler map(RecordVO recordVO) throws Exception {
        log.info("找到了吗?");
        Optional<FileParseHandler> first = fileParseHandlers.stream().filter(fileParseHandler -> fileParseHandler.getFileEnum().getFileNameParentFolder().equals(recordVO.getFile_path())).findFirst();
        if (first.isPresent()) {
            FileParseHandler fileParseHandler = first.get();
            fileParseHandler.setRecordVO(recordVO);
            log.info("找到了");
            return fileParseHandler;
        }
        return null;
    }
}
