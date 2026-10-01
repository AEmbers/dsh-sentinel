# dsh-sentinel

[English](README.md) | 中文

条件驱动的唤醒，给 [DeepSeek Harness](https://github.com/deepseek-ai/deepseek-harness) 用：agent 注册一条 watch 就可以去睡觉——甚至直接关掉会话——条件成立时由 sentinel 把它叫醒。每一次订阅、每一次触发都是用户可见的会话事件，浏览器 dock 上随时能看到谁在值守。

![Sentinel dock 面板（展开）](docs/preview/sentinel-panel.png)

## 工作原理

Node 侧持有一个与 server 同生命周期的运行时：把插件自己的 sidecar 日志（`$DSH_HOME/sentinel.jsonl`）折叠成活跃订阅，按共享的 5 秒心跳逐个探测传感器，命中后走官方 followup 通道投递唤醒——必要时先复活休眠会话的 agent。所以订阅能扛住进程重启；server 停机期间变真的条件，会在下一次探测时补触发。

值守是常驻进程的事：探测和触发投递只在有一个长期运行的 dsh 进程（通常是 `dsh web`）时进行。一次性 headless 运行也能加载插件、创建/列出/取消 watch，但进程退出后没人探测——等下一个常驻进程起来，这些 watch 自动恢复值守。

每个 `$DSH_HOME` 只有一个值守 owner：租约文件 `sentinel.lease` 让第一个进程拥有探测和投递权；同一 home 上的第二个 dsh 进程保持被动（工具可用，写入照常落到共享 sidecar），owner 死后一个租约 TTL 内接管。owner 每个心跳重读 sidecar，被动实例上创建的 watch 会被自动收编。投递语义是 at-least-once：崩溃前已记录但没送出的触发，重启后从 `delivered` 水位线重新入队。

浏览器侧是 composer 上方的 dock 卡片（`conversation.input.dock` 族），列出本会话的活跃 watch——传感器、目标、实时探测状态、触发预算、下次探测倒计时——展开还有最近的触发历史。它轮询只读的 state 路由；会话没有 watch 时不渲染任何东西。

两个界面暴露 server 全局的 watch 集合。**侧边栏全局面板列表**里多出一个哨兵条目（`sidebar.panellist`，id `sentinel`），点击即在中栏（`main` 同名 key）打开 watch 表——只要有活跃 watch，图标上就带一个状态点。dashboard 是跨所有会话的全量 watch 表：会话（active/dormant）、传感器、目标、pattern、触发预算、最近探测状态、下次探测。

![全局 dashboard](docs/preview/sentinel-dashboard.png)

## 传感器

| kind | 引擎 | 触发条件 |
| --- | --- | --- |
| `file` | 路径快照 + inotify 推送 | 快照变化（亚秒级）；fs 事件加速 |
| `command` | 只读 shell 单行输出，按间隔探测 | 输出/退出码变化 |
| `http` | 按间隔探测 URL | 状态/响应体变化 |
| `process` | `pgrep -f` 模式，按间隔探测 | 匹配集变化 |
| `port` | 对 `[host:]port` 做 TCP 连接，按间隔探测 | 可达性变化（open/closed/timeout） |
| `webhook` | 纯推送 | 对返回的 hook URL 发任何 POST |

带 `pattern` 时，探测类传感器在该正则的"不匹配→匹配"边沿触发，webhook 只接受匹配的载荷；不带时，探测类传感器对基线之后的任何变化触发。

## 配置

所有部署相关的旋钮都在插件的 config schema 里（括号内为默认值），在 profile 的 `cordis.patch.yml` 里对 bundle 行覆盖：

```yaml
- id: dsh-sentinel
  name: dsh-sentinel
  config:
    heartbeatMs: 5000            # 探测轮间隔
    probeConcurrency: 8          # 每轮并发探测数
    maxSubscriptionsPerSession: 16
    maxPendingWakeups: 8         # 每会话排队唤醒上限，超出丢最旧的
    defaultIntervalSeconds: 30   # watch 未指定间隔时的默认值（5–86400）
    defaultCooldownSeconds: 60
    dutyLeaseTtlMs: 30000        # owner 死后被动实例的接管窗口
    notifyWebhookUrl: ''         # 可选：每次触发以 JSON POST 到这里
    restartCommand: ''           # 重启守卫用什么命令拉起宿主（默认复用本进程自己的命令行）
    restartIdleTimeoutMs: 120000 # 等当前回合结束再动手杀进程的最长时间
    restartRespawnGraceMs: 8000  # 杀完之后等外部守护把宿主拉回来的宽限期
```

非法值会让插件加载时以 schema 错误失败，而不是运行时乱来。`dutyLeaseTtlMs` 必须至少是 `heartbeatMs` 的两倍，否则运行时直接拒绝启动——owner 每个心跳续租一次，租约 TTL 短于心跳就意味每个周期里都有一段过期窗口，第二个实例会在窗口里抢到 duty、和 owner 一起投递同一次唤醒。

`notifyWebhookUrl` 把每次触发以 JSON POST（`{plugin, event, sessionId, id, kind, target, note, fireNumber, maxFires, summary, after}`）送出 harness——指向飞书/企微/Slack 机器人或任意接收端都行。这条投递是 at-most-once：POST 失败只在日志里 warn，绝不阻塞 harness 内的唤醒。

## 工具

- `sentinel_watch` — 注册 watch：`kind`、`target`、可选 `pattern`、`interval`（1–3600 秒，默认 30）、`note`（随每次唤醒原样送达）、`maxFires`（默认 1：一次性）、`cooldown`（默认 60 秒）、可选 `ttl`。
- `sentinel_list` — 列出活跃 watch 及其实时探测状态。
- `sentinel_cancel` — 按 id 取消一条 watch。
- `sentinel_restart` — 重启 dsh 宿主，并在它回来后把这个会话唤醒（见下）。

### 重启宿主并自己回来

`sentinel_restart({ note, confirm: true })` 专门解决插件平时做不到的那件事：插件就住在它要重启的那个进程里。
装插件、改宿主配置、重编原生模块——这些都要重启才生效，而重启通常意味着只能请人去点一下。

交接过程刻意让「动手杀进程」那一半留在宿主外面，而「活下来的那一半」不需要任何进程在跑：

1. 工具先注册一条盯着 `$DSH_HOME/sentinel-restart/ready-<id>.flag` 的一次性 `file` watch，**并且在动手之前先探测一次、
   把这个 watch 的基线落盘**。fold 只在订阅的第一次观测时记录基线，少了这一步，重启如果早于第一个心跳发生，
   替代宿主就没有可比对的基线——它会把标记文件吸收成自己的基线而不是触发它。
2. 一个 detached 的守卫进程（分两次 spawn，脱离了宿主的父 PID 链，否则会被自己发起的杀进程带走）先等调用它的
   agent 走到 **idle 边沿**。这一步才是重启不会截断「发起重启的那个回合」的原因。
3. 守卫杀掉宿主进程树，在宿主停机期间写下标记文件，然后先等 `restartRespawnGraceMs`——桌面端这类守护进程
   可能会自己把宿主子进程拉回来，再拉一份就会撞端口。
4. 替代宿主折叠 sidecar、重新播种探测基线，看到标记文件相对 `<absent>` 是一次真实的快照变化，于是带着你的便签
   唤醒那个休眠会话。

标记文件是两个进程之间**全部**的接口：没有端口、没有 URL、没有需要对齐的就绪握手。当工具无法确定重启命令
（`process.argv` 里没有入口脚本、也没配 `restartCommand`）时它会**拒绝执行**，而不是杀掉一个没人拉得起来的宿主。

因为杀进程发生在 idle 边沿，请把 `sentinel_restart` 放在回合最后调用，然后**立刻结束回合**——之后的任何操作都会被截断。
它会带走整个 dsh 宿主：它服务的每个会话，以及任何占着端口的插件。这里的「重启」就是这个意思。

## 路由

- `GET /plugins/dsh-sentinel/state?sessionId=…` — dock 和侧边栏面板用的只读状态（省略 `sessionId` 返回所有会话）。
- `GET /plugins/dsh-sentinel/dashboard` — server 全局 watch 表。
- `POST /plugins/dsh-sentinel/hook?id=watch-N&s=<sessionId>` — webhook 入口；把一条 `curl` 塞进 CI 任务、git hook 或另一台机器的脚本，就能叫醒 agent。watch id 按会话隔离，`s` 限定符保证两个会话的 `watch-1` hook 不打架（工具直接发完整 URL）。不带 `s` 的 URL 仍可用，解析到第一条匹配的 webhook watch。
- `POST /plugins/dsh-sentinel/cancel?sessionId=…&id=watch-N` — 手动取消。dashboard 表和每个 UI 行都带 ✕，任何 watch 都能手动停掉——包括会话和 agent 早就不在了的孤儿 watch；host 没有 session-deleted 事件，所以这是最后的兜底开关。
- 四条路由都带浏览器信任围栏：浏览器标记的跨站请求（恶意页面可以往 localhost form-POST）和 DNS rebinding 尝试（Host/Origin 指向 DNS 主机名）一律 403。`curl`、CI 任务这类无头客户端不受影响。state 路由还返回每个会话的 `duty`（租约心跳年龄）和 `droppedWakeups`（被 `maxPendingWakeups` 上限丢掉的排队唤醒）。

首次探测语义：不带 pattern 的 watch 把第一次观测吸收为基线（不触发）；带 pattern 的 watch 如果目标已经匹配，第一次探测就触发——条件本来就成立。

## 兼容性

在以下宿主版本上实测通过（插件加载、duty 租约持有、web 路由应答均正常）：

- `0.2.0-rc.2` —— 2026-10-01，Windows 桌面端实测：harness 依赖范围重新钉到 0.2.0 线，`@deepseek-ai/schemastery`
  对齐到宿主的 `^3.18.4`——0.2.0 的 schema 类型面更严，`Schema<Config>` 不再接受 `meta.default` 带 schemastery
  新版 `Volatile` 标记的 schema。顺带查出并修掉两个 Windows 缺陷：对「路径里还留着 8.3 短名成分」（如
  `C:\Users\ADMINI~1\...`）的**目录**做 `fs.watch` 会让 libuv 直接 abort 整个进程——
  `Assertion failed: !_wcsnicmp(filename, dir, dlen), file src\win\fs-event.c, line 72`，退出码 0xC0000409，
  且 try/catch 拦不住——所以 file watch 的挂载现在先用 `realpath`（Windows 上走原生实现，会展开短名）
  规范化，并对无法证明安全的目录挂载直接放弃、退回心跳轮询；另一处是 command 探测的测试夹具用了
  POSIX 专有的 `printf`/`exit`。实测：`pnpm typecheck` 干净，Windows 上全部 70 个测试通过，
  其中包括此前会把测试进程直接 abort 掉的 e2e 文件推送测试。顺带把套件里两个潜在 flake 也一并关掉了，
  而不是绕过去：duty-owner 测试原本用 `heartbeatMs: 500` 配 `dutyLeaseTtlMs: 400`，这个组合下
  「单一 owner」的保证在每个周期里有约 20% 时间**按构造就不成立**（运行时现在会直接拒绝这种配置）；
  另有几处测试用固定 sleep 等「file watch 的第一次探测必须在被监视文件出现之前落地」，
  断言的其实是「调度器赏脸」——现在改为轮询 sidecar 里那条持久化基线行。改动后连续 29 轮全量跑里只有 1 轮失败、
  且之后 20 轮再没复现（那一轮与一次并发构建重叠），改动前大约 3 轮里就有 1 轮失败。harness import 也从
  `dependencies` 改成了 `peerDependencies`——见下面「为什么 harness 依赖是 peer 而不是 dependency」，
  这正是消除 dsh-market 那条宿主依赖警告、并让插件不再把 `@deepseek-ai/dsh-tools` / `dsh-llm` 从宿主手里占走的原因
- `0.1.7-rc.2` —— 2026-09-29，对线上 profile 的副本做整轮净装升级彩排：0.1.7 删除了共享的兜底 `plugin` 消息来源 kind（改为每个生产者声明自己的），因此唤醒携带 `{ kind: 'sentinel' }`——在会话流里落位同为 `context`，两条版本线上都渲染为 "Sentinel"。harness 依赖范围也重新钉到 0.1.7 线：严格 semver 下 `>=0.1.5-rc.2 <0.2.0` **不包含** `0.1.7-rc.2`（预发布规则），若不改，0.1.7 宿主会把本插件的 harness import 解析到 0.1.5 的副本——正是 0.1.5 对齐时消除掉的那类漂移。实测：`pnpm typecheck` 与全部 63 个测试通过，插件激活并持有 duty 租约，web 路由应答正常，下发的客户端 bundle 含 `sidebar.panellist`（boot 图 65 条）
- `0.1.5-rc.2` —— 2026-09-15，对齐 0.1.5 后的正式 web 部署实测：插件整条运行时 import 闭包都解析到部署线（harness 依赖改为显式 dependencies，profile 里更旧的 hoisted 副本再也遮不住它们），客户端半侧去掉 shim 后按真实 0.1.5 类型构建，`pnpm typecheck` 与全部 63 个测试通过，线上文件 watch 在改动后 1s 内经 inotify 触发，唤醒作为 plugin 来源的会话消息投递进会话；重启后部署下发的是新的客户端半侧（bundle rev 变更、含 `sidebar.panellist`、boot 图 54 条）
- `0.1.5-alpha.2` —— 2026-09-09，临时 web profile 实测：Node 插件加载、duty 租约、state/dashboard 路由和浏览器插件 bundle 均正常，浏览器控制台无报错；`conversation.input.dock` 仍是有效的会话级 list slot，插件 sidecar 不受 Session V3 迁移影响
- `0.1.1-rc.2` —— 2026-08-26，源码构建冒烟：git 装入 web profile，duty 租约持有，state 与 dashboard 路由应答正常
- `0.1.0-rc.8` —— 2026-08-20，scratch profile 冒烟
- `0.1.0-rc.7` —— 2026-08-20，正式 web 部署

这里的兼容指 cordis loader 条目、`ctx.agents` 跟进通道、所声明的 slot 座位和 web 路由持续可用；若某版本破坏了其中任一环节，请提 issue。

### 为什么 harness 依赖是 peer 而不是 dependency

`@deepseek-ai/dsh-tools` 和 `@deepseek-ai/dsh-llm` 是本插件运行时真正 import 的两个宿主包（`defineTool`、`createUserMessage`），它们声明为 **peerDependencies**。这不是表面功夫。

`dsh-app-boot` 的 `createRuntimeResolution` 会从两个 scope 构建插件加载时的解析表：installation anchor（宿主自带的 `@deepseek-ai/*` 副本）和 profile。而 `installedProfilePackageNames` 会收集 profile 里**实际存在于磁盘上的直接依赖**——它自己的注释写得很清楚：「installed direct dependencies that Node resolves before profile fallback」——并把它们当作 `reserved`，于是这些名字会**从宿主那一半的解析表里被抹掉**。何况 Node 本来就会先解析 profile 里那份。

所以插件把宿主核心包声明成普通 dependency，并不只是多带了一份副本：它等于**把这个名字从宿主手里抢走**，profile 里其他所有消费者都受影响。

dsh-market 记录了这个真实观测到的后果，并把这几个名字列进 `KNOWN_SHARED_HOST_PACKAGES`：「the dsh-excel-chat failure mode where the plugin's copy gets hoisted to the profile root and shadows the host's version（tool calls die, minimal preset fails to mount）」。旧装法正是把 `@deepseek-ai/dsh-llm`、`dsh-tools`、`dsh-scope` 连同六个传递依赖一起放到了 profile 根目录，正好落在这个模式里。

改成 peer 之后，插件绑定到宿主那唯一的实例，同时 DSH 自己的兼容性检查器会拿 peer 范围去核对正在运行的宿主——版本差距从「静默漂移」变成「可见警告」。它们仍留在 `devDependencies` 里，所以本仓库单独 typecheck / build 不受影响。`@deepseek-ai/dsh-scope` 直接删掉了：没有任何地方 import 它。

> 这一条**推翻了**上面 0.1.5 / 0.1.7 记录里的推理——那两版特意把 harness import 做成 *dependencies*，就是为了不让 profile 里 hoisted 的副本遮蔽它们。那个担忧是真实的，但它是冲着「未声明的解析」去的；声明成 peer 才是同一意图的正确写法，而 dependency 那种写法带着那两版记录没有考虑到的代价。

冷加载实测：一个全新的 `headless` profile，装上本插件，`node_modules` 里**完全没有** `@deepseek-ai/dsh-tools` / `dsh-llm`，照样能启动，`sentinel_list` 也能调用——这两个包由宿主经解析表提供。

## 安装


走官方 bundle 通道一行装完：

```sh
dsh plugin --profile web add dsh-sentinel
```

或者直接从 git 装（构建产物直接提交在仓库里，git 源安装不需要跑构建）：

```sh
dsh plugin --profile web add "github:fuhefei/dsh-sentinel#v0.11.0"
```

或者手动加 node 半边：在你现有 base 上叠一层 patch-list 配置：

```yaml
# cordis.patch.yml
- insert:
    - id: dsh-sentinel
      name: dsh-sentinel
```

浏览器半边在同一个包里（`./client`），由 Web UI 的插件加载器注入。

### 侧边栏界面（0.1.5 及以后）

dock、侧边栏条目和 dashboard 在原版 host 上都能用：条目注册进官方 `sidebar.panellist` 座位，面板注册进布局的 `main` slot，无需给宿主打任何补丁。

在 0.1.2 及更早版本里，全局视图改而长在每条被监视会话行下方，需要官方树从未声明过的会话行扩展洞；该路径已退役，`patches/session-row-holes.patch` 只为那些旧树保留。0.1.5 起旧洞已不存在，面向它的插件必须改用上面的面板座位。

### better-sidebar 集成（可选）

同一 profile 里装有 [dsh-better-sidebar](https://github.com/omdsh-dev/DSH-better-sidebar) 时，sentinel 通过它公开的 `ctx.betterSidebar.registerTab` 扩展面，把全局 watch 表注册成一个侧边栏 tab（`dsh-sentinel:watches`，在 **+** 菜单里）：server 上每条 watch 的实时探测状态、触发预算和最近触发历史，由一个共享轮询器供数。无需配置；没装 better-sidebar 时注册静默跳过，dock / 面板 / dashboard 照常工作。

![better-sidebar 工作台里的 sentinel tab](docs/preview/sentinel-better-sidebar-tab.png)

### 组合使用

和 [dsh-notification](https://github.com/omdsh-dev/dsh-notification) 一起装，整个唤醒回路就能到达桌面：sentinel 叫醒 agent，agent 干完这一轮，回合结束触发桌面通知——零集成代码，两个插件自己组合出来。

## 开发

```sh
npm install
npm run build     # tsc -b + tsdown (lib/index.js, lib/client.js)
npm test          # vitest: domain fold/normalize、传感器、dashboard 转义、e2e 唤醒流程
```

## 许可证

BSD-3-Clause，见 [LICENSE](LICENSE)。
