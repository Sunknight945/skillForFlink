# flink_cdc  



## cdc 名称解释

- **数据集成领域（最常见）**: 在数据库和数据仓库技术中，**CDC链路**指的是**变更数据捕获（Change Data Capture）**的数据同步通道。
   简单来说，它就像一条为数据建造的“实时物流通道”
- **核心作用**：能够实时捕获源数据库中的每一次数据变化（如插入、更新、删除操作），并将这些变更以事件流的形式，准确、高效地同步到下游系统（如数据仓库、分析平台或其他数据库）。
- **工作原理**：不同于传统定时全量搬运，CDC通过解析数据库的事务日志（如MySQL的binlog）来工作，只捕获并传输变更部分，因此对源库压力小、延迟低，能实现秒级甚至亚秒级的数据同步。
- **典型应用**：例如，配置一条从业务数据库到分析平台的CDC链路，实现实时报表、实时大屏或数据灾备。阿里云的PolarDB和Flink CDC就是此类技术的具体实践



# flink_cdc 前期准备工作(docker 单机版)

## 一、Docker 网络准备

创建一个自定义网络，让所有容器能够通过容器名互相访问。

```bash
docker network create my-kafka-network
```



## 二、MySQL 容器（开启 binlog，支持 CDC）

### 1. 启动 MySQL 5.7 容器（挂载数据卷，开启 binlog）

```bash
docker run -d \
  --name mysql \
  --network my-kafka-network \
  -p 3306:3306 \
  -v E:\docker\dockerData\mysql\data:/var/lib/mysql \
  -e MYSQL_ROOT_PASSWORD=123456 \
  mysql:5.7 \
  --server-id=1 \
  --log-bin=mysql-bin \
  --binlog-format=ROW \
  --binlog-row-image=FULL
```



### 2. 验证 binlog 开启

```bash
docker exec -it mysql mysql -uroot -p123456 -e "SHOW VARIABLES LIKE 'log_bin';"
# 应显示 ON
```



### 3. 创建测试数据库和表

```sql
CREATE DATABASE test_cdc;
USE test_cdc;
CREATE TABLE users (
    id INT PRIMARY KEY AUTO_INCREMENT,
    name VARCHAR(100)
);
INSERT INTO users (name) VALUES ('alice'), ('bob');
```





## 三、Kafka 容器（KRaft 模式，内外双监听器）

### 1. 启动 Kafka 4.0.0 容器

```bash
docker run -d \
  --name kafka \
  --network my-kafka-network \
  -p 9092:9092 \
  -p 9094:9094 \
  -v E:\docker\dockerData\kafka\data:/var/lib/kafka/data \
  -e KAFKA_NODE_ID=1 \
  -e KAFKA_PROCESS_ROLES=broker,controller \
  -e KAFKA_LISTENERS=PLAINTEXT://0.0.0.0:9092,CONTROLLER://0.0.0.0:9093,EXTERNAL://0.0.0.0:9094 \
  -e KAFKA_ADVERTISED_LISTENERS=PLAINTEXT://kafka:9092,EXTERNAL://localhost:9094 \
  -e KAFKA_LISTENER_SECURITY_PROTOCOL_MAP=PLAINTEXT:PLAINTEXT,EXTERNAL:PLAINTEXT,CONTROLLER:PLAINTEXT \
  -e KAFKA_CONTROLLER_LISTENER_NAMES=CONTROLLER \
  -e KAFKA_CONTROLLER_QUORUM_VOTERS=1@localhost:9093 \
  -e KAFKA_LOG_DIRS=/var/lib/kafka/data \
  -e KAFKA_OFFSETS_TOPIC_REPLICATION_FACTOR=1 \
  apache/kafka:4.0.0
```



### 2. 验证 Kafka 正常

```bash
docker logs kafka --tail 20
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
```





## 四、Debezium 容器（Kafka Connect）

### 启动 Debezium Connect 2.4 容器

```bash
docker run -d \
  --name debezium \
  --network my-kafka-network \
  -p 8083:8083 \
  -v E:\docker\dockerData\debezium\data:/kafka/data \
  -e GROUP_ID=1 \
  -e CONFIG_STORAGE_TOPIC=my_connect_configs \
  -e OFFSET_STORAGE_TOPIC=my_connect_offsets \
  -e STATUS_STORAGE_TOPIC=my_connect_statuses \
  -e BOOTSTRAP_SERVERS=kafka:9092 \
  quay.io/debezium/connect:2.4
```

### 2. 提交 MySQL 连接器配置



创建 `mysql-connector.json`：

```json
{
  "name": "mysql-connector",
  "config": {
    "connector.class": "io.debezium.connector.mysql.MySqlConnector",
    "tasks.max": "1",
    "database.hostname": "mysql",
    "database.port": "3306",
    "database.user": "root",
    "database.password": "123456",
    "database.server.id": "1",
    "topic.prefix": "mysql-cdc",
    "database.include.list": "test_cdc",
    "schema.history.internal.kafka.bootstrap.servers": "kafka:9092",
    "schema.history.internal.kafka.topic": "schema-changes.mysql",
    "include.schema.changes": "true",
    "snapshot.mode": "initial"
  }
}
```

提交：

```bash
curl -X POST http://localhost:8083/connectors \
  -H "Content-Type: application/json" \
  -d @mysql-connector.json
```



### 3. 验证连接器状态

```bash
curl -s http://localhost:8083/connectors/mysql-connector/status | jq .
# 预期 tasks 状态为 RUNNING
```





## 五、连通性验证

### 1. Debezium 容器内测试访问 MySQL 和 Kafka

```bash
docker exec -it debezium bash
(echo >/dev/tcp/mysql/3306) 2>/dev/null && echo "MySQL OK"
(echo >/dev/tcp/kafka/9092) 2>/dev/null && echo "Kafka OK"
exit
```



### 2. 宿主机测试 Kafka 外部端口

```powershell
Test-NetConnection localhost -Port 9094
# 应显示 TcpTestSucceeded : True
```



### 3. 查看 Kafka 中自动创建的主题

```bash
docker exec -it kafka /opt/kafka/bin/kafka-topics.sh --list --bootstrap-server localhost:9092
# 应该看到 mysql-cdc.test_cdc.users
```



### 4. 手工消费验证（确保 MySQL 插入后能收到 JSON 消息）

```bash
# 终端1：启动消费者
docker exec -it kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server kafka:9092 \
  --topic mysql-cdc.test_cdc.users \
  --from-beginning

# 终端2：在 MySQL 中插入数据
docker exec -it mysql mysql -uroot -p123456 -e "INSERT INTO test_cdc.users (name) VALUES ('cdc_test');"

# 消费者终端应出现 JSON 消息
```





## 六、ClickHouse 准备（供 Flink 写入）

### 1. 启动 ClickHouse（已有容器 clickhouse24，确保端口映射）

```bash
docker run -d --name clickhouse24 \
  -p 8123:8123 -p 9000:9000 \
  -v E:/docker/dockerData/clickhouse/data:/var/lib/clickhouse \
  clickhouse/clickhouse-server:24.3.2.23
```

### 2. 创建目标表（支持版本和删除标记）

```sql
CREATE TABLE test_cdc.users2 (
    id Int64,
    name String,
    _version UInt64,
    _deleted UInt8
) ENGINE = ReplacingMergeTree(_version)
ORDER BY id;
```



### 3. （可选）将 ClickHouse 也加入 my-kafka-network，以便后续 Flink 容器访问；但 Flink 在你的宿主机上运行，所以通过 localhost:8123 即可，无需入网。







## 七、Flink 作业前最终检查清单

- MySQL 容器运行，binlog ON，test_cdc.users 表存在。
- Kafka 容器运行，端口 9094 在宿主机可通。
- Debezium 容器运行，连接器状态 RUNNING。
- Kafka 主题 `mysql-cdc.test_cdc.users` 存在，且 MySQL 新增数据能立即被 `kafka-console-consumer` 看到。
- ClickHouse 容器运行，`test_cdc.users2` 表已创建。
- 所有容器已加入 `my-kafka-network`（MySQL、Kafka、Debezium）以保证内部容器名解析。
- 你本地 Java 项目已包含 Flink 相关依赖（flink-streaming-java, flink-connector-kafka, flink-connector-jdbc, clickhouse-jdbc, fastjson 等）。

至此，环境已完全就绪，可以运行 Flink 作业消费 Kafka 并写入 ClickHouse。你的作业代码（`Kafka2ClickHouseDelete`）已经成功跑通，并能处理 INSERT/UPDATE/DELETE。如果需要回顾具体代码或配置细节，我可以继续提供帮助。



