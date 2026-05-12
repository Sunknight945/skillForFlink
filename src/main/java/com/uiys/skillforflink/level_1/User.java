package com.uiys.skillforflink.level_1;

import lombok.Data;

import java.io.Serializable;

/**
 * @author uiys
 */
@Data
public class User implements Serializable {
    private Long id;
    private String name;

    public User(Long id, String name) {
        this.id = id;
        this.name = name;
    }
}
