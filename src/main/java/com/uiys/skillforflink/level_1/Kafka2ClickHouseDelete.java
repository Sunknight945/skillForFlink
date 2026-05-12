package com.uiys.skillforflink.level_1;

import com.alibaba.fastjson2.JSONObject;
import lombok.Data;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Objects;

/**
 * @author uiys
 * 这个环节使用到了 mysql -> debezium -> kafka -> flink -> clickhous.
 */
public class Kafka2ClickHouseDelete {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 使用 KafkaSource 新 API
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("localhost:9094")
                .setTopics("mysql-cdc.test_cdc.users")
                .setGroupId("flink-cdc-writer")
                .setStartingOffsets(OffsetsInitializer.latest())
                .setDeserializer(new NullSafeKafkaRecordSchema())
                .setProperty("enable.auto.commit", "false")
                .build();

        DataStream<String> rawStream = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source")
                .filter(Objects::nonNull);

        // 解析 JSON 并转换为 ClickHouseRow
        DataStream<ClickHouseRow> userStream = rawStream
                .map(jsonStr -> {

                    JSONObject root = JSONObject.parseObject(jsonStr);
                    JSONObject payload = root.getJSONObject("payload");
                    String op = payload.getString("op");
                    JSONObject after = payload.getJSONObject("after");
                    JSONObject before = payload.getJSONObject("before");

                    ClickHouseRow row = new ClickHouseRow();
                    row._version = System.currentTimeMillis(); // 可改为 payload.getLong("ts_ms")
                    if ("c".equals(op) || "u".equals(op)) {
                        if (after == null) {
                            return null;
                        }
                        row.id = after.getLong("id");
                        row.name = after.getString("name");
                        row._deleted = 0;
                    } else if ("d".equals(op)) {
                        if (before == null) {
                            return null;
                        }
                        row.id = before.getLong("id");
                        row.name = before.getString("name");
                        row._deleted = 1;
                    } else {
                        return null;
                    }
                    return row;
                });

        // 写入 ClickHouse
        userStream.addSink(JdbcSink.sink(
                "INSERT INTO test_cdc.users2 (id, name, _version, _deleted) VALUES (?, ?, ?, ?)",
                (statement, row) -> {
                    statement.setLong(1, row.id);
                    statement.setString(2, row.name);
                    statement.setLong(3, row._version);
                    statement.setByte(4, row._deleted);
                },
                JdbcExecutionOptions.builder().withBatchSize(100).withBatchIntervalMs(2000).build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:clickhouse://localhost:8123/test_cdc")
                        .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
                        .build()
        ));

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
