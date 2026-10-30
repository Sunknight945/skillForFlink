package com.uiys.skillforflink.level3;

import com.baomidou.mybatisplus.annotation.TableId;
import lombok.Data;

@Data
public class FileRecord {
    @TableId
    public String fileName;
    public String filePath;
    public String fileInsertCount;
}