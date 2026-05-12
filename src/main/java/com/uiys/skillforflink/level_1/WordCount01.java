package com.uiys.skillforflink.level_1;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author uiys
 */
public class WordCount01 {

    /**
     * 1. env - 准备环境
     * 2. source- 加载数据
     * 3. transformation - 数据处理转换
     * 4. sink - 数据输出
     * 5. execute - 执行
     */
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();


        env.setRuntimeMode(RuntimeExecutionMode.STREAMING);


        DataStreamSource<String> dataStream01 = env.fromElements("spark flink kafka", "spark sqoop flink", "kafka hadoop flink");


        SingleOutputStreamOperator<String> flatMapStream = dataStream01.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String line, Collector<String> collector) throws Exception {
                String[] arr = line.split(" ");
                for (String word : arr) {
                    collector.collect(word);
                }
            }
        });


        SingleOutputStreamOperator<Tuple2<String, Integer>> mapStream = flatMapStream.map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String word) throws Exception {
                return Tuple2.of(word, 1);
            }
        });

        DataStream<Tuple2<String, Integer>> sumResult = mapStream.keyBy(new KeySelector<Tuple2<String, Integer>, String>() {
            @Override
            public String getKey(Tuple2<String, Integer> tuple2) throws Exception {
                return tuple2.f0;
            }
            // 此处的1 指的是元组的第二个元素，进行相加的意思


        }).sum(1);

        System.out.println("应该执行完了?");
        sumResult.print().setParallelism(1);
        System.out.println("应该执行完了?");


        env.execute();

    }

}
