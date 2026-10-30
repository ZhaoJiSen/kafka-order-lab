# Kafka 订单实验室 · 可视化前端

独立前端，用一笔真实订单观察 PHP → Kafka → Java → Kafka → PHP，再按已记录的步骤回看。采用 Oil UI 的「订单实验台」方向：左侧创建/选择订单，中间看流程，下方看分区，详情与日志按需展开。

## UI 组件约定

使用 **HeroUI 3.2.6**（`@heroui/react` + `@heroui/styles`）。业务区域由 Card/Header/Content 组织；表单用 Form/TextField/Label/Input；最近订单用 ListBox；模式与日志来源用 ToggleButtonGroup；Topic 用 Tabs；倍速与日志级别用 Select；状态用 Chip；详情用 Drawer；处理记录与日志展开用 Accordion；按订单过滤用 Checkbox。

组件的圆角、表面、交互状态和动效沿用 HeroUI。只通过主题变量设置品牌色，不再用全局 `.button`、`.input` 重画组件皮肤。中文状态徽标使用 13px，保证阅读尺寸。

页面分栏使用 Tailwind Grid/Flex。HeroUI 不提供页面栅格；Kafka 专用的分区轨道、记录位置和 React Flow 消息路径仍采用业务组件。新增通用控件先查看已安装 HeroUI 的组件 API，避免再手写一份。

## 启动前端

本目录使用 **nvm + Node.js 24 + pnpm 12.15.0**。`.nvmrc` 指定 Node 24，`package.json` 声明 Node 24.x 与 pnpm 版本；依赖以 `pnpm-lock.yaml` 为准，不混用 npm 锁文件。不修改 nvm 的全局默认版本。

```bash
cd kafka-order-lab/frontend
nvm use
pnpm install
pnpm dev
```

打开 <http://127.0.0.1:5173>。这个命令同时启动 Vite 和本机观察服务（3010）。Vite 代理 `/api/lab`，浏览器不直接访问 Kafka 或 Docker。

首次在其他机器使用、尚未安装 Node 24 时，先运行 `nvm install`。`pnpm dev:web` 只启动 Vite，不会启动观察服务；如果选择分开运行，还需要在另一个终端执行 `pnpm dev:server`。业务服务已经启动，但 3010 没有运行时，网页无法取得任何服务状态。

## 连接当前的本机 PHP

默认配置就是当前学习方式，无需创建 `.env`：

| 来源 | 默认地址 |
| --- | --- |
| PHP API | `http://127.0.0.1:8000` |
| Java API | `http://127.0.0.1:18081` |
| Kafka | `127.0.0.1:19092` |
| PHP 日志 | `../php-laravel/storage/logs/laravel.log` |

完整链路需要 Kafka、Java 以及 PHP 的三个进程。已经运行的进程不用重复启动。 项目改名后是一个例外：若 PHP 两个常驻进程在改名前已启动，应在原终端停止后，从 `kafka-order-lab/php-laravel` 重新启动，以刷新内存中的日志路径。观察服务读取 `php-laravel/storage/logs/laravel.log`，不读取旧目录名下的文件。

Kafka 沿用当前服务。需要启动 Kafka 时，在项目根目录执行：

```bash
docker compose up -d --no-build kafka topics
```

Java 在本机的另一个终端启动（Java 21 + Maven，详见 [Java 说明](../java-review/README.md)）：

```bash
cd kafka-order-lab/java-review
./dev.sh
```

前端默认分别读取本机 PHP 和 Java 日志，Java 的 HTTP 地址仍为 `127.0.0.1:18081`。Java 进程内已包含消费和 Outbox 定时发送，无需为 Java 单独启动第二个业务进程。

然后在 `php-laravel` 目录的三个终端分别运行：

```bash
php artisan serve --host=127.0.0.1 --port=8000
```

```bash
KAFKA_BROKERS=127.0.0.1:19092 php artisan demo:publish-outbox
```

```bash
KAFKA_BROKERS=127.0.0.1:19092 php artisan demo:consume-results
```

第一次准备 PHP 环境、安装扩展和创建数据库，请按根目录 README 第 2 节执行。

## 如果使用 Docker PHP

复制 `.env.example` 为本目录的 `.env`，将：

```dotenv
PHP_API_URL=http://127.0.0.1:18080
PHP_RUNTIME=docker
```

随后重启 `pnpm dev`。业务容器按根目录 `./scripts/up.sh` 的方式启动。**同一时间选用一种 PHP 运行方式**：本机 SQLite 与 Docker 数据卷不是同一数据库，对应的结果消费者不能混着运行。

若同时使用 Docker Java，在 `frontend/.env` 另设 `JAVA_RUNTIME=docker`；PHP 与 Java 的运行模式分别配置。Java 从 Docker 切换到本机时，还需要同步 H2 数据库，见 Java 说明。

前端只连本机回环地址；观察服务不会自动启停业务服务、重置消费组或删除数据。

## 怎么使用

1. 创建订单。总额不超过 1000 元时审核通过，超过时拒绝。
2. 路径中的节点根据真实处理记录更新。已有记录的节点可点击或用键盘打开，查看结果、来源与相关位置；等待节点不可打开空详情。Kafka 节点直接显示本单的实际分区与 Offset，原始 JSON 可展开。
3. 在分区视图寻找蓝色“本单”记录。格内数字是消息 Offset；绿色指针表示消费组已提交的下一位置；末端表示下一写入位置。
4. 在两个 Topic 之间切换，比较 PHP 发出的创建事件与 Java 发回的审核结果。PHP 与 Java 分区算法不同，两个 Topic 中的编号不要求相同。
5. 切到“教学回放”，逐步、自动播放或暂停。回放按已采集的真实证据揭示过程；不会再次发消息、重置消费位置或回滚数据库。进入时固定本单证据，新采集到的步骤通过“载入新步骤”加入；回放期间创建按钮暂停，切回实时观察后可继续创建。
6. “运行日志”可以按 PHP / Java / Kafka、级别与内容筛选，暂停画面更新或复制记录。所有时间在页面中按 Asia/Shanghai 显示。

处理记录按业务步骤排列。业务保存/消息记录时间与消费组查询时间分别标注；查询时间不是该组实际执行 commit 的精确时刻。

## 数据来源与边界

- 订单与 PHP 发送、接收结果：原有 Laravel HTTP API，保留原始接口、学习注释与 Blade 页面。
- Java 审核状态：原有 Java 只读 HTTP API；无需改审核逻辑或新增 Java 接口。
- 消息、分区边界、消费组提交位置：真实 Kafka。专属 `order-lab-observer-*` 消费者不加入业务组，关闭自动提交。
- 每分区读取最近 100 条消息并继续观察新记录，界面显示窗口范围；观察窗口不代表全部历史。
- 回放记录保存在本目录 `.local/history.json`，最多 2000 个步骤，保留所选记录附近的上下文；该目录已忽略。首次打开时，已有数据库记录只能补充已知步骤，不能还原未采集过的每个瞬间。
- PHP 与 Java 本机日志分别增量读取，处理日志截断和轮转；各自的 Docker 模式改为跟踪固定容器名。Broker 日志来自 Kafka 容器。容器重启后自动重新跟踪，重复历史行去重。
- Kafka Broker 默认不会为每笔订单输出一行日志。业务写入确认来自 PHP/Java，不能当成 Broker 原生日志。
- Java 读取进度只能以已处理的输入记录及 Kafka 已提交位置证明，不能把采集服务的读取位置冒充业务消费者的实时内部位置。
- 消费组不存在或未提交时显示未知；连接失败保留上次成功数据并标注，不用 0 代替失败。
- 新版 Node 上 KafkaJS 2.2.4 可能输出 `TimeoutNegativeWarning`。这是其请求队列定时器与 Node 的兼容警告；本次真实读写验证通过。观察服务不修改该依赖的内部代码。

## 验证

```bash
pnpm typecheck
pnpm lint
pnpm test
```

真实链路验证（会创建 6 笔有“验收”前缀的演示订单，需要三个业务进程及 Kafka/Java 正常）：

```bash
pnpm exec tsx tests/live-smoke.ts
```

该脚本连续运行 3 轮通过/拒绝订单，核对幂等键、消息内容和位置、消费组进度，以及只读回放和观察消费者隔离。原有根目录 Docker smoke 使用另一套运行环境，不要指向本机 SQLite。

构建命令为 `pnpm build`；本次发布已经通过类型检查、lint、测试与生产构建。本次发布检查见根目录 `docs/release-review.md`。
