# Operor API

Operor 在 recipe 中配置 `api_port` 后启动一个 HTTP/WebSocket 服务器，供前端实时获取仪器采样数据。

```yaml
pipeline:
  api_port: 8080
  record: all
```

所有响应均携带 `Access-Control-Allow-Origin: *`，可直接从浏览器跨域访问。

---

## HTTP

### GET /status

返回当前运行状态和最新一帧的采样值。

**响应** `200 application/json`

```json
{
  "running": true,
  "frame_count": 42,
  "columns": ["voltage", "current", "$ITER"],
  "latest": {
    "voltage": "1.230",
    "current": "0.045",
    "$ITER": "41"
  }
}
```

| 字段 | 类型 | 说明 |
|---|---|---|
| `running` | bool | pipeline 是否仍在采样 |
| `frame_count` | number | 累计采集的帧数 |
| `columns` | string[] | 所有列名，顺序与 WS 推送一致 |
| `latest` | object | 最新帧各列的值；未采集到的列不出现在对象中 |

`latest` 在任何帧到达之前为空对象 `{}`。

---

### GET /columns

仅返回列名列表，比 `/status` 更轻量。

**响应** `200 application/json`

```json
["voltage", "current", "$ITER"]
```

---

## WebSocket

连接地址：`ws://<host>:<api_port>`，标准 WebSocket 握手，无需额外子协议。

### 连接后行为

连接建立后服务器立即开始推送数据，默认每 **100 ms** 批量发送一次（批量模式）。

### 服务端 → 客户端：帧批次

每次推送一条 text 消息，包含本批次所有采集帧。

```json
{
  "frames": [
    {
      "fields": [
        { "name": "voltage", "value": "1.230" },
        { "name": "current", "value": "0.045" }
      ]
    },
    {
      "fields": [
        { "name": "voltage", "value": "1.231" },
        { "name": "current", "value": "0.046" }
      ]
    }
  ]
}
```

- `frames` 数组按时间顺序排列，**不丢帧**——批次窗口内的每一帧都会到达。
- 某列在某帧无值时，对应的 `fields` 条目不出现（稀疏）。
- 批次大小取决于采样速率和 `flush_ms`，最多 256 帧/批。

### 客户端 → 服务端：配置消息

连接后任意时刻可发送 text 消息调整刷新率：

```json
{ "flush_ms": 200 }
```

| `flush_ms` 值 | 行为 |
|---|---|
| `0` | 即时模式：每采集到一帧立即推送（`frames` 数组始终只有 1 个元素） |
| `> 0` | 批量模式：每隔 N 毫秒推送一次，N 毫秒内的所有帧打包在一条消息里 |

配置立即生效，无需重新连接。未知字段被忽略，可安全扩展。

---

## 注意事项

- **缓冲区溢出**：每个 WS 客户端有独立的 256 帧缓冲区。若客户端消费速度持续落后于采样速率，最旧的帧会被静默丢弃——此后的帧仍正常到达，只是中间有缺口。
- **连接断开**：服务器检测到写失败时停止向该客户端推送，连接自动关闭，其他客户端不受影响。
- **pipeline 结束后**：`running` 变为 `false`，WS 连接保持打开，`/status` 仍可查询最终快照，直到服务进程退出。

---

## 快速上手（浏览器）

```js
// 一次性查询当前状态
const status = await fetch('http://localhost:8080/status').then(r => r.json());
console.log(status.latest);

// 实时订阅，每 200ms 接收一批帧
const ws = new WebSocket('ws://localhost:8080');

ws.onopen = () => {
  ws.send(JSON.stringify({ flush_ms: 200 }));
};

ws.onmessage = (event) => {
  const { frames } = JSON.parse(event.data);
  for (const frame of frames) {
    const row = Object.fromEntries(frame.fields.map(f => [f.name, f.value]));
    console.log(row); // { voltage: "1.230", current: "0.045" }
  }
};

ws.onclose = () => console.log('disconnected');
```
