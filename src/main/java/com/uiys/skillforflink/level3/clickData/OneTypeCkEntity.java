package com.uiys.skillforflink.level3.clickData;

import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;
import org.springframework.util.DigestUtils;

import java.io.Serializable;
import java.nio.charset.StandardCharsets;

@Data
@TableName("one_type_ck_entity")
public class OneTypeCkEntity implements Serializable {
    private String sourceFileName;
    private String fieldOne;
    private String fieldTwo;
    private String fieldThree;
    private String md5;
    private String ckInputTime;

    public void CalculateMd5AndSet() {
        String s = getFieldOne() + getFieldTwo() + getFieldThree();
        String md5 = DigestUtils.md5DigestAsHex(s.getBytes(StandardCharsets.UTF_8));
        setMd5(md5);
    }


}
