package com.uiys.skillforflink.level_1;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;

public class FlinkSQLCDCToClickHouse {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inStreamingMode().build();
        StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

        // 创建 Kafka 源表
        tEnv.executeSql(
            "CREATE TABLE kafka_cdc_source (\n" +
            "  id INT,\n" +
            "  name STRING,\n" +
            "  op STRING,\n" +
            "  ts_ms BIGINT\n" +
            ") WITH (\n" +
            "  'connector' = 'kafka',\n" +
            "  'topic' = 'mysql-cdc.test_cdc.users',\n" +
            "  'properties.bootstrap.servers' = 'localhost:9094',\n" +
            "  'properties.group.id' = 'flink-sql-cdc',\n" +
            "  'format' = 'debezium-json',\n" +
            "  'debezium-json.schema-include' = 'false',\n" +
            "  'scan.startup.mode' = 'latest-offset'\n" +
            ")"
        );

        // 创建 ClickHouse 结果表
        tEnv.executeSql(
            "CREATE TABLE clickhouse_users (\n" +
            "  id INT,\n" +
            "  name STRING,\n" +
            "  _version BIGINT,\n" +
            "  _deleted TINYINT\n" +
            ") WITH (\n" +
            "  'connector' = 'jdbc',\n" +
            "  'url' = 'jdbc:clickhouse://localhost:8123/test_cdc',\n" +
            "  'table-name' = 'users2',\n" +
            "  'driver' = 'com.clickhouse.jdbc.ClickHouseDriver',\n" +
            "  'sink.buffer-flush.max-rows' = '100',\n" +
            "  'sink.buffer-flush.interval' = '2s'\n" +
            ")"
        );

        // 执行插入
        tEnv.executeSql(
            "INSERT INTO clickhouse_users\n" +
            "SELECT \n" +
            "  COALESCE(id, 0) AS id,\n" +
            "  COALESCE(name, '') AS name,\n" +
            "  ts_ms AS _version,\n" +
            "  IF(op = 'd', 1, 0) AS _deleted\n" +
            "FROM kafka_cdc_source\n" +
            "WHERE op IN ('c', 'u', 'd')\n" +
            "  AND id IS NOT NULL"
        );
    }
}