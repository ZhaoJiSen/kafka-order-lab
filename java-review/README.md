# 本机运行 Java 订单审核服务

使用 Java 21 + Maven，服务由你在终端中启动。PHP、Java 都可以在本机运行；Kafka 仍连接已有的 `127.0.0.1:19092`。

本机已安装 Java 21 和 Maven 后，在此目录执行：

```bash
cd kafka-order-lab/java-review
./dev.sh
```

脚本只给当前进程选择 Homebrew 或 macOS 已安装的 Java 21，随后执行 `mvn -s settings-local.xml spring-boot:run -Dspring-boot.run.profiles=local`。不会修改全局 Java 版本或终端配置。首次运行 Maven 会下载依赖，看到 `Started OrderReviewApplication` 后表示启动完成。按 `Ctrl+C` 停止。

`settings-local.xml` 只供这个 Demo 使用，依赖从 HTTPS Maven Central 下载，避开本机公司项目的私服配置；不会改动 `~/.m2/settings.xml`。

VS Code / VS Code Insiders 的 Java 插件也应将 `java.configuration.maven.userSettings` 指向本项目的 `settings-local.xml`，按自己的克隆路径配置；仓库不提交个人编辑器设置。

如果编辑器仍显示旧私服的 `Non-resolvable parent POM` 错误，执行命令面板中的 `Java: Clean Java Language Server Workspace` 并按提示重启语言服务，让它重新导入项目。该操作刷新编辑器的 Java 工作区，不要求重启业务服务。

其他机器缺少工具时，可先运行 `brew install openjdk@21 maven`。

## 本机配置

`application-local.yml` 覆盖容器配置，启用后使用：

| 项目 | 地址 / 路径 |
| --- | --- |
| HTTP | `http://127.0.0.1:18081` |
| Kafka | `127.0.0.1:19092` |
| H2 数据库 | `.local/data/reviews.mv.db` |
| Java 日志 | `.local/logs/java-review.log`，同时输出到终端 |

从其他目录启动时，也可调用此脚本的完整路径；脚本会先进入 `java-review`。`.local/` 中的数据不提交到代码仓库。

检查服务：

```bash
curl http://127.0.0.1:18081/actuator/health
```

返回 `{"status":"UP"}` 表示 HTTP 服务已启动；Kafka 消费是否正常还需结合终端日志和网页的消费进度判断。

查询一笔订单的审核：

```bash
curl http://127.0.0.1:18081/api/reviews/ord-替换为实际订单编号
```

同一个 Java 进程中，`OrderCreatedListener` 消费创建事件，`ReviewService` 审核并保存数据库，`OutboxPublisher` 定时把审核结果发送回 Kafka，无需为 Java 再开一个 Outbox 命令。

## 从收到订单开始读代码

代码先按订单业务归在 `demo.order` 下，再按职责拆包：

```text
src/main/java/demo/order/
├── OrderReviewApplication.java     # 启动入口，位于扫描根包
├── config/                        # Kafka 错误处理配置
├── controller/                    # HTTP 查询接口
│   └── ReviewController.java
├── service/                       # 审核业务及金额规则
│   ├── ReviewService.java
│   └── ReviewRule.java
├── entity/                        # 对应数据库表的实体
│   ├── Review.java
│   ├── ProcessedEvent.java
│   └── OutboxEvent.java
├── repository/                    # 实体查询和保存
│   ├── ReviewRepository.java
│   ├── ProcessedEventRepository.java
│   └── OutboxEventRepository.java
└── messaging/                     # Kafka 接收、解析与发送
    ├── OrderCreatedListener.java
    ├── EventCodec.java
    ├── OutboxPublisher.java
    └── event/                     # PHP 与 Java 交换的消息结构
        ├── OrderCreatedEvent.java
        └── OrderReviewedEvent.java
```

Java 的目录对应包名，例如 `service/ReviewService.java` 声明 `package demo.order.service;`，其他包通过 `import` 引用它。启动类保留在 `demo.order`，Spring Boot 可以默认扫描下面的组件、实体和 Repository。测试也分别放在 `service/`、`messaging/`、`integration/` 子包。

这是一种适合当前 Demo 的组织方式。Spring Boot 不强制固定目录层级；业务增加后，可以与 `order` 并列增加其他业务包。参考 [Spring Boot 3.5：代码组织](https://docs.spring.io/spring-boot/3.5/reference/using/structuring-your-code.html)。

数据库访问使用 **Spring Data JPA + Hibernate ORM**。Entity 描述一条数据库记录，Repository 提供查询和保存，Service 把这些操作串成业务流程。可以与 Laravel 的模型查询、保存操作对照着理解；Java 这里把实体与查询接口拆开。

按这个顺序阅读：

1. [OrderCreatedListener.java](src/main/java/demo/order/messaging/OrderCreatedListener.java)：Spring Kafka 后台消费线程拉取消息，再调用 `receive()`。PHP 把消息发给 Kafka，Java 通过自己的 Kafka 客户端读取。
2. [ReviewService.java](src/main/java/demo/order/service/ReviewService.java)：先用 `existsById()` 去重，再审核，最后用 `save()` 保存结果和待发送事件。
3. [ReviewRule.java](src/main/java/demo/order/service/ReviewRule.java)：总额不超过 1000 元通过，超过则拒绝。
4. [Review.java](src/main/java/demo/order/entity/Review.java) 与 [ReviewRepository.java](src/main/java/demo/order/repository/ReviewRepository.java)：前者对应 `reviews` 表的一行，后者继承 JPA 提供的查询、保存方法，由 Spring 创建实现。
5. [OrderReviewedEvent.java](src/main/java/demo/order/messaging/event/OrderReviewedEvent.java)：发回 PHP 的消息结构，保留本次收到的 Topic、Partition、Offset、Key。
6. [OutboxPublisher.java](src/main/java/demo/order/messaging/OutboxPublisher.java)：每轮最多读取 20 条待发送事件，Kafka 确认后保存实际分区、Offset 和发送时间。

三个实体与数据表对应：

| 实体 | 数据表 | 作用 |
| --- | --- | --- |
| `ProcessedEvent` | `processed_events` | 记录已处理事件和来源位置，避免重复处理 |
| `Review` | `reviews` | 保存订单审核结果 |
| `OutboxEvent` | `outbox_events` | 保存需要发给 Kafka 的结果消息及发送状态 |

`ReviewService.handle()` 上的 `@Transactional(rollbackFor = Exception.class)` 将三张表的本次修改放在同一个事务内。处理失败时一起回滚，包括消息序列化抛出的受检异常；方法经 Spring 事务代理正常返回后，Listener 才提交 Kafka 消费位置。

ORM 继续使用原 `schema.sql` 和已有 H2 数据，`ddl-auto: validate` 只检查映射，不自动重建或更新表。时间字段显式对应原有 `TIMESTAMP`，消息内容仍用 `CLOB`；查询接口和 Kafka 消息的 JSON 字段保持兼容。

修改 Java 代码后，在你启动 Java 的终端按 `Ctrl+C`，重新执行 `./dev.sh` 生效。首次切换到 ORM 时 Maven 会下载 JPA/Hibernate 依赖。

本机测试可执行（无需启动 Kafka）：

```bash
JAVA_HOME=/opt/homebrew/opt/openjdk@21/libexec/openjdk.jdk/Contents/Home \
PATH=/opt/homebrew/opt/openjdk@21/bin:$PATH \
mvn -s settings-local.xml test
```

持久化测试使用独立 H2 内存库，沿用原建表脚本并校验映射，Kafka 发送用测试替身；测试覆盖重复消息、重复订单、事务回滚、发送失败重试，以及旧 JDBC 时间和大文本数据的兼容性。

参考：[Spring Kafka 监听容器](https://docs.spring.io/spring-kafka/reference/kafka/receiving-messages/kafkalistener-lifecycle.html)、[Spring Data JPA 查询方法](https://docs.spring.io/spring-data/jpa/reference/jpa/query-methods.html)。

## 与原 Docker 环境的关系

本机与 Docker Java 使用相同的业务消费组 `demo-java-order-reviewers`，但默认使用不同路径的 H2 数据库。切换时应先停止原 Java 服务，并迁移原数据库，保证已提交的消费进度仍有对应的业务记录。不要通过重置消费组来代替数据库迁移。

已有数据的环境切换运行方式前，应先停止原实例，再迁移数据库并核对消费进度，避免两套数据库各自继续写入后再来回切换。

前端观察服务默认 `JAVA_RUNTIME=native`，读取此目录下的本机日志；如以后切回 Docker Java，需在 `frontend/.env` 设置 `JAVA_RUNTIME=docker` 并重启前端。

配置方式参考 [Spring Boot Profiles](https://docs.spring.io/spring-boot/3.5/reference/features/profiles.html) 与 [Logging](https://docs.spring.io/spring-boot/reference/features/logging.html)。
