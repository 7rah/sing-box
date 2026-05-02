### 结构

```json
{
  "type": "selector",
  "tag": "select",

  "outbounds": [
    "proxy-a",
    "proxy-b",
    "proxy-c"
  ],
  "default": "proxy-c",
  "load_balance": {
    "enabled": false,
    "instances": 3,
    "strategy": "round_robin"
  },
  "interrupt_exist_connections": false
}
```

!!! quote ""

    选择器目前只能通过 [Clash API](/zh/configuration/experimental/clash-api/) 来控制。

### 字段

#### outbounds

==必填==

用于选择的出站标签列表。

#### default

默认的出站标签。默认使用第一个出站。

#### load_balance

在选中的出站的多个实例之间负载均衡连接。

启用后，`instances` 表示每个可选择出站的总实例数，包含原始出站。目前仅支持 `round_robin` 策略。

#### interrupt_exist_connections

当选定的出站发生更改时，中断现有连接。

仅入站连接受此设置影响，内部连接将始终被中断。
