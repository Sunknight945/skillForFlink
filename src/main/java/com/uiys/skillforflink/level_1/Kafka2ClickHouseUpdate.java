package com.uiys.skillforflink.level_1;

import com.alibaba.fastjson2.JSONObject;
import lombok.Data;
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
public class Kafka2ClickHouseUpdate {
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

        DataStream<ClickHouseRow> userStream = rawStream.map((MapFunction<String, ClickHouseRow>) jsonStr -> {
            JSONObject root = JSONObject.parseObject(jsonStr);
            JSONObject payload = root.getJSONObject("payload");
            String op = payload.getString("op");
            JSONObject after = payload.getJSONObject("after");
            JSONObject before = payload.getJSONObject("before");

            ClickHouseRow row = new ClickHouseRow();
            row.id = after != null ? after.getLong("id") : (before != null ? before.getLong("id") : null);
            if (row.id == null) {
                return null;
            }

            // 使用 Kafka 消息时间戳作为版本号（ms）
            row._version = System.currentTimeMillis();

            if ("c".equals(op)||"u".equals(op)) {
                row.name = after.getString("name");
                row._deleted = 0;
            } else {
                return null;
            }
            return row;
        }).filter(Objects::nonNull);


        // 3. 写入 ClickHouse (JDBC Sink)
        userStream.addSink(JdbcSink.sink(
                "INSERT INTO test_cdc.users2 (id, name, _version, _deleted) VALUES (?, ?, ?, ?)",
                (statement, row) -> {
                    statement.setLong(1, row.id);
                    statement.setString(2, row.name);
                    statement.setLong(3, row._version);
                    statement.setByte(4, row._deleted);
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

        env.execute("CDC Full Operation to ClickHouse");
    }

    @Data
    public static class ClickHouseRow {
        public Long id;
        public String name;
        public Long _version;
        public Byte _deleted;
    }

}
