package com.uiys.skillforflink.level_1;

import com.alibaba.fastjson2.JSON;
import com.alibaba.fastjson2.JSONObject;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Objects;
import java.util.Properties;

/**
 * @author uiys
 * 这个环节使用到了 mysql -> debezium -> kafka -> flink -> clickhous.
 */
public class Kafka2ClickHouseInsert {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 1. Kafka 消费者配置
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "localhost:9094");
        kafkaProps.setProperty("group.id", "flink-cdc-writer");
        // 必须关闭自动提交，由 Flink checkpoint 管理 offset (实现 exactly-once)
        kafkaProps.setProperty("enable.auto.commit", "false");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "mysql-cdc.test_cdc.users",
                new SimpleStringSchema(),
                kafkaProps
        );
        consumer.setStartFromLatest();   // 从最新消息开始消费

        DataStream<String> rawStream = env.addSource(consumer);
        // rawStream.print();   // 直接打印 Kafka 原始 JSON

        DataStream<User> userStream = rawStream.map((MapFunction<String, User>) jsonStr -> {
            JSONObject root = JSON.parseObject(jsonStr);
            JSONObject payload = root.getJSONObject("payload");
            if (Objects.isNull(payload)) {
                return null;
            }
            JSONObject after = payload.getJSONObject("after");
            if (Objects.isNull(after)) {
                return null;
            }
            Long id = after.getLong("id");
            String name = after.getString("name");
            return new User(id, name);
        }).filter(Objects::nonNull);


        // 3. 写入 ClickHouse (JDBC Sink)
        userStream.addSink(JdbcSink.sink(
                "INSERT INTO test_cdc.users (id, name) VALUES (?, ?)",
                (statement, user) -> {
                    statement.setLong(1, user.getId());
                    statement.setString(2, user.getName());
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(100)          // 每100条批量写入，提高性能
                        .withBatchIntervalMs(2000)   // 或每2秒批量写入一次
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:clickhouse://localhost:8123/test_cdc")
                        .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
                        // ClickHouse 默认无需用户名密码，如需要可添加：
                        // .withUsername("default")
                        // .withPassword("")
                        .build()
        ));

        // 可选：同时打印到控制台，便于观察
        userStream.print();

        env.execute("Kafka CDC Consumer");
    }
}