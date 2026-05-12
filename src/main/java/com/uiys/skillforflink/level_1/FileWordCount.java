package com.uiys.skillforflink.level_1;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author uiys
 */
public class FileWordCount {


    public static void main(String[] args) throws Exception {
        System.out.println("VM options: " + System.getProperty("sun.java.command"));

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> stream = env.readTextFile("E:\\CodeRepositories\\javaTest\\skillForFlink\\skillForFlink\\src\\main\\resources\\fileWord.txt");


        stream
                .flatMap((String line, Collector<String> out) -> {
                    for (String word : line.split(" ")) {
                        out.collect(word);
                    }
                })
                .returns(Types.STRING)
                .map(word -> Tuple2.of(word, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(t -> t.f0)
                .sum(1)
                .print();

        env.execute("fileCount");

    }

}
