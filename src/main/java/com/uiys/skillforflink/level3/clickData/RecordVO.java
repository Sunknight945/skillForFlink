package com.uiys.skillforflink.level3.clickData;

import lombok.Data;

import java.io.Serializable;

@Data
public class RecordVO implements Serializable {
    private String file_name;
    private String file_path;

    public RecordVO(String file_name, String file_path) {
        this.file_name = file_name;
        this.file_path = file_path;
    }
}
