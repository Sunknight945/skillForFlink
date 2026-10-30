package com.uiys.skillforflink.level3;

import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Optional;
import java.util.stream.Stream;


@RequestMapping("scheduledFileRecord")
@RestController
public class ScheduledFileRecord {


    @Resource
    FileRecordMapper fileRecordMapper;

    @PostMapping("parse")
    public void parse() throws IOException {
        Path path = Paths.get("src/main/resources/fileFolder");
        try (Stream<Path> walk = Files.walk(path, 1)) {
            walk.forEach(folder -> {
                String string = folder.getFileName().toString();
                System.out.println(string);
                System.out.println(folder);
                boolean directory = new File(folder.toString()).isDirectory();
                if (directory) {
                    Optional<FileEnum> first = Arrays.stream(FileEnum.values()).filter(en -> en.getFileNameParentFolder().equals(string)).findFirst();
                    if (first.isPresent()) {
                        System.out.println("这个时候就是指定的文件了.");
                        File file = new File(folder.toString());
                        if (file.exists() && file.isDirectory()) {
                            File[] files = file.listFiles();
                            assert files != null;
                            for (File file1 : files) {
                                System.out.println(file1.getName());
                                if (!file1.isDirectory()) {
                                    FileRecord fileRecord = new FileRecord();
                                    if (fileRecordMapper.selectById(file1.getName()) == null) {
                                        fileRecord.setFileName(file1.getName());
                                        fileRecord.setFilePath(first.get().fileNameParentFolder);
                                        fileRecordMapper.insert(fileRecord);
                                    }
                                }
                            }
                        }
                    }
                }
            });
        }

    }


}
