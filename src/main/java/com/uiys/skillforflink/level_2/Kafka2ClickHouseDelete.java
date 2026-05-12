package com.uiys.skillforflink.level_2;

import com.alibaba.fastjson.JSONObject;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.Properties;

public class Kafka2ClickHouseDelete {

    // 定义侧输出流标签，用于收集解析失败的消息
    private static final OutputTag<String> INVALID_MESSAGE_TAG = new OutputTag<String>("invalid-message") {
    };

    public static void main(String[] args) throws Exception {

        // 在 main 方法开头
        Configuration conf = new Configuration();
// 明确指定 Web UI 端口
        conf.setInteger(RestOptions.PORT, 8081);
// 可选：增加连接超时时间（毫秒）
        conf.setString(RestOptions.BIND_PORT, "8081");
// 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI(conf);


        // ==================== Checkpoint 配置 ====================
        // 开启 Checkpoint，每 60 秒一次
        env.enableCheckpointing(60000L);
        // Exactly-Once 语义
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // 两次 Checkpoint 之间最小间隔 30 秒
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(30000L);
        // Checkpoint 超时时间 10 分钟
        env.getCheckpointConfig().setCheckpointTimeout(600000L);
        // 同时最多 1 个 Checkpoint
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        // 作业取消时保留 Checkpoint 以便恢复
        env.getCheckpointConfig().enableExternalizedCheckpoints(
                CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION
        );
        // 失败后重启策略：固定延迟重启，尝试 3 次，每次间隔 10 秒
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.seconds(10)));

        // 可选：设置状态后端（如果使用 RocksDB，需要添加依赖）
        // env.setStateBackend(new RocksDBStateBackend("file:///tmp/flink-checkpoints"));
        // 或者使用文件系统状态后端
        // env.getCheckpointConfig().setCheckpointStorage("file:///tmp/flink-checkpoints");

        // ==================== Kafka 消费者配置 ====================
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", "localhost:9094");
        kafkaProps.setProperty("group.id", "flink-cdc-writer");
        kafkaProps.setProperty("enable.auto.commit", "false");

        FlinkKafkaConsumer<String> consumer = new FlinkKafkaConsumer<>(
                "mysql-cdc.test_cdc.users",
                // 自己实现处理 null 的 schema
                new NullSafeStringSchema(),
                kafkaProps
        );
        consumer.setStartFromLatest();

        DataStream<String> rawStream = env.addSource(consumer)
                // 过滤掉 null（tombstone）
                .filter(Objects::nonNull);

        // ==================== 使用 ProcessFunction 处理正常消息和坏消息 ====================
        SingleOutputStreamOperator<ClickHouseRow> userStream = rawStream
                .process(new ProcessFunction<String, ClickHouseRow>() {
                    @Override
                    public void processElement(String jsonStr, Context ctx, Collector<ClickHouseRow> out) {
                        try {
                            ClickHouseRow row = parseJsonToRow(jsonStr);
                            if (row != null) {
                                out.collect(row);
                            }
                        } catch (Exception e) {
                            // 将坏消息发送到侧输出流
                            ctx.output(INVALID_MESSAGE_TAG, jsonStr);
                            System.err.println("Parse error for message: " + jsonStr);
                            e.printStackTrace();
                        }
                    }
                });

        // 处理侧输出流中的坏消息（这里简单打印，实际可写入 Kafka、日志或告警）
        DataStream<String> invalidStream = userStream.getSideOutput(INVALID_MESSAGE_TAG);
        invalidStream.print();   // 打印到控制台，颜色可能为红色

        // ==================== 写入 ClickHouse ====================
        // 这里保持你原来的 JdbcSink 代码（见之前的 Kafka2ClickHouseDelete）
        userStream.addSink(JdbcSink.sink(
                "INSERT INTO test_cdc.users2 (id, name, _version, _deleted) VALUES (?, ?, ?, ?)",
                (statement, row) -> {
                    statement.setLong(1, row.id);
                    statement.setString(2, row.name);
                    statement.setLong(3, row._version);
                    statement.setByte(4, row._deleted);
                },
                JdbcExecutionOptions.builder()
                        .withBatchSize(100)
                        .withBatchIntervalMs(2000)
                        .build(),
                new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:clickhouse://localhost:8123/test_cdc")
                        .withDriverName("com.clickhouse.jdbc.ClickHouseDriver")
                        .build()
        ));

        // 可选：打印正常消息便于观察
        userStream.print();

        env.execute("CDC Full Operation to ClickHouse with Checkpoint and SideOutput");
    }

    // ==================== 解析 JSON 的逻辑 (与原 map 中完全相同) ====================
    private static ClickHouseRow parseJsonToRow(String jsonStr) {
        JSONObject root = JSONObject.parseObject(jsonStr);
        JSONObject payload = root.getJSONObject("payload");
        if (payload == null) {
            return null;
        }
        String op = payload.getString("op");
        JSONObject after = payload.getJSONObject("after");
        JSONObject before = payload.getJSONObject("before");

        ClickHouseRow row = new ClickHouseRow();
        // 时间戳版本号
        long version = System.currentTimeMillis();

        if ("c".equals(op) || "u".equals(op)) {
            if (after == null) {
                return null;
            }
            row.id = after.getLong("id");
            row.name = after.getString("name");
            row._deleted = 0;
            row._version = version;
        } else if ("d".equals(op)) {
            if (before == null) {
                return null;
            }
            row.id = before.getLong("id");
            row.name = before.getString("name");
            row._deleted = 1;
            row._version = version;
        } else {
            // 忽略其他操作类型（如 r, snapshot）
            return null;
        }
        return row;
    }

    // ==================== 自定义反序列化器（处理 null 消息） ====================
    public static class NullSafeStringSchema implements DeserializationSchema<String> {
        @Override
        public String deserialize(byte[] message) {
            if (message == null) {
                return null;
            }
            return new String(message, StandardCharsets.UTF_8);
        }

        @Override
        public boolean isEndOfStream(String nextElement) {
            return false;
        }

        @Override
        public TypeInformation<String> getProducedType() {
            return TypeInformation.of(String.class);
        }
    }

    // ==================== ClickHouse 行对象 ====================
    public static class ClickHouseRow {
        public Long id;
        public String name;
        public Long _version;
        public Byte _deleted;
    }
}