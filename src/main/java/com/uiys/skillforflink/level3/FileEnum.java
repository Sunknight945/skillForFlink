package com.uiys.skillforflink.level3;

import lombok.Getter;

@Getter
public enum FileEnum {

    FirstFileName("第一种", 1,"firstType"),
    SecondFileName("第二种", 2,"secondType");

    public String fileTypeName;
    public Integer fileNameCode;
    public String fileNameParentFolder;

    public void setFileTypeName(String fileTypeName) {
        this.fileTypeName = fileTypeName;
    }

    public void setFileNameCode(Integer fileNameCode) {
        this.fileNameCode = fileNameCode;
    }

    public void setFileNameParentFolder(String fileNameParentFolder) {
        this.fileNameParentFolder = fileNameParentFolder;
    }

    FileEnum(String fileTypeName, Integer fileNameCode, String fileNameParentFolder) {
        this.fileTypeName = fileTypeName;
        this.fileNameCode = fileNameCode;
        this.fileNameParentFolder = fileNameParentFolder;
    }



}
