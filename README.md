# Keepalive

WebSocket 远程主机保活客户端，用于维持与远程主机的长连接。

## 功能特性

- WebSocket 连接管理（自动重连、指数退避）
- 远程主机保活（心跳检测、断线重连）
- 多主机支持（在线主机列表、序号选择）
- 信号处理（SIGHUP 忽略、Ctrl+C 优雅退出）

## 编译

```bash
# 编译 Linux 版本
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o keepalive-linux-amd64 keepalive.go

# 编译当前平台版本
go build -o keepalive keepalive.go
```

## 使用方法

```bash
# 交互模式（手动选择主机）
./keepalive-linux-amd64

# 指定主机序号
./keepalive-linux-amd64 -c 1

# 自定义保活间隔（秒）
./keepalive-linux-amd64 -c 1 -t 15

# 后台运行
nohup ./keepalive-linux-amd64 -c 1 &

# 结束进程
pkill keepalive
```

## 参数说明

| 参数 | 说明 | 默认值 |
|------|------|--------|
| `-c` | 目标主机序号（跳过交互选择） | 空（交互选择） |
| `-t` | 保活轮询秒数 | 60 |

## 运行流程

```
1. 连接 WebSocket 服务器
2. 获取在线主机列表
3. 选择目标主机
4. 连接远程主机
5. 进入保活循环
   - WS心跳：每15秒
   - 主机检查：每interval秒
   - 断线重连：自动
```

## 依赖

- [github.com/gorilla/websocket](https://github.com/gorilla/websocket)

## 许可证

MIT License
