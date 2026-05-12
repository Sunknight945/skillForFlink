package com.uiys.skillforflink.level_1;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @author uiys
 * 这个环节使用到了 mysql -> debezium -> kafka -> flink 就是这个环节.
 *
 */
public class Kafka2ClickHousePrint {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "localhost:9094");   // 注意端口改为 9094
        kafkaProps.setProperty("group.id", "flink-test-group");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "mysql-cdc.test_cdc.users",
                new SimpleStringSchema(),
                kafkaProps
        );
        consumer.setStartFromLatest();   // 从最新消息开始消费

        DataStream<String> stream = env.addSource(consumer);
        stream.print();   // 直接打印 Kafka 原始 JSON

        env.execute("Kafka CDC Consumer");
    }
}