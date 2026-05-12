package com.uiys.skillforflink.level_1;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @author uiys
 */
public class SocketWordCount {


    public static void main(String[] args) throws Exception {

        // 1. 创建一个线程，在后台启动一个简单的 Socket 服务端
        // new Thread(() -> {
        //     try (ServerSocket serverSocket = new ServerSocket(9999)) {
        //         Socket socket = serverSocket.accept();
        //         PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
        //         // 模拟发送几行数据给 Flink
        //         out.println("spark flink kafka");
        //         out.println("spark sqoop flink");
        //         out.println("kafka hadoop flink");
        //     } catch (IOException e) {
        //         e.printStackTrace();
        //     }
        // }).start();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> stream = env.socketTextStream("localhost", 9998);

        stream.flatMap((String line, Collector<String> out) -> {
                    for (String word : line.split(" ")) {
                        out.collect(word);
                    }
                }).returns(Types.STRING)
                .map(word -> Tuple2.of(word, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(t -> t.f0)
                .sum(1)
                .print();

        env.execute("socket word count!");


    }

}
