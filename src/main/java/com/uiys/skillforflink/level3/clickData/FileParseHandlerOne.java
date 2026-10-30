package com.uiys.skillforflink.level3.clickData;

import com.uiys.skillforflink.level3.FileEnum;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

@Slf4j
@Component
public class FileParseHandlerOne implements FileParseHandler {

    private RecordVO recordVO;



    private OneTypeCkEntityMapper oneTypeCkEntityMapper;


    @Override
    public void setRecordVO(RecordVO recordVO) {
        this.recordVO = recordVO;
    }

    @Override
    public RecordVO getRecordVO() {
        return recordVO;
    }


    @Override
    public FileEnum getFileEnum() {
        return FileEnum.FirstFileName;
    }

    @Override
    public void readFile() {
        this.oneTypeCkEntityMapper = SpringContextHolder.getApplicationContext().getBean(OneTypeCkEntityMapper.class);
        try (BufferedReader br = new BufferedReader(new FileReader(recordVO.getFile_name()))) {
            List<String> batch = new ArrayList<>(1000);
            String line;
            int lineNum = 0;
            while ((line = br.readLine()) != null) {
                lineNum++;
                // 每一行都是完整的一条数据
                // 比如: "1001|50000|2024-01-15 10:30:45"
                batch.add(line);
                if (batch.size() >= 1000) {
                    processBatch(recordVO.getFile_name(), batch, lineNum - 999, lineNum);  // 处理这批
                    batch = new ArrayList<>(1000);
                }
            }
            if (!batch.isEmpty()) {
                processBatch(recordVO.getFile_name(), batch, lineNum - batch.size() + 1, lineNum);
            }
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void processBatch(String fileName, List<String> batch, int i, int lineNum) {
        List<OneTypeCkEntity> collect = batch.stream().map(item -> {
            String[] split = item.split("\\$\\$");
            OneTypeCkEntity oneTypeCkEntity = new OneTypeCkEntity();
            oneTypeCkEntity.setSourceFileName(fileName);
            oneTypeCkEntity.setFieldOne(split[0]);
            oneTypeCkEntity.setFieldTwo(split[1]);
            oneTypeCkEntity.setFieldThree(split[2]);
            oneTypeCkEntity.setCkInputTime(LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
            oneTypeCkEntity.CalculateMd5AndSet();
            return oneTypeCkEntity;
        }).collect(Collectors.toList());
        List<String> md5sExist = oneTypeCkEntityMapper.selectExistMd5s(collect.stream().map(OneTypeCkEntity::getMd5).collect(Collectors.toList()));
        List<OneTypeCkEntity> collect1 = collect.stream().filter(item -> !md5sExist.contains(item.getMd5())).collect(Collectors.toList());
        Integer i1 = oneTypeCkEntityMapper.batchInsert(collect1);
        log.info("one_type_ck_entity 写入了 {} 条数据!", i1);
    }


}
