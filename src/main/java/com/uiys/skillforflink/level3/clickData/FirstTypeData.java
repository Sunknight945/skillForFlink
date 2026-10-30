package com.uiys.skillforflink.level3.clickData;

import lombok.Data;

import java.io.Serializable;

@Data
public class FirstTypeData implements Serializable {
    private String firstName;
    private String lastName;
    private String email;
}
