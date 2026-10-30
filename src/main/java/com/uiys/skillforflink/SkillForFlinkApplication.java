package com.uiys.skillforflink;

import org.mybatis.spring.annotation.MapperScan;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

/**
 * @author uiys
 */
@MapperScan("com.uiys.skillforflink.level3")
@SpringBootApplication
public class SkillForFlinkApplication {

    public static void main(String[] args) {
        SpringApplication.run(SkillForFlinkApplication.class, args);
    }

}
