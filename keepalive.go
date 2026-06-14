package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/gorilla/websocket"
)

var (
	interval int
	hostID   string
	wsURL    = "ws://127.0.0.1:33272/ws"
)

var (
	pclistCh = make(chan string, 16)
	pcinfoCh = make(chan string, 4)
	conpcCh  = make(chan string, 4)
	conerrCh = make(chan string, 4)
	disconCh = make(chan string, 4)
	errCh    = make(chan error, 4)
)

func init() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage of %s:\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  -c string\n")
		fmt.Fprintf(os.Stderr, "    	目标主机序号（跳过交互选择）\n")
		fmt.Fprintf(os.Stderr, "  -t int\n")
		fmt.Fprintf(os.Stderr, "    	保活轮询秒数（默认60）\n")
		fmt.Fprintf(os.Stderr, "\n使用说明:\n")
		fmt.Fprintf(os.Stderr, "  后台运行: nohup %s -c 1 &\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "  结束进程: pkill keepalive\n")
	}
}

func main() {
	flag.IntVar(&interval, "t", 60, "保活轮询秒数，默认60")
	flag.StringVar(&hostID, "c", "", "目标主机序号（跳过交互选择）")
	flag.Parse()

	conn := connect()
	routerDone := startRouter(conn)

	// ── 获取主机列表 ──
	safeSend(conn, "pclist<$!$>")
	resp, ok := waitMsg(pclistCh, 15*time.Second)
	if !ok {
		log.Fatalf("[错误] pclist 无响应")
	}
	hosts := parseHostList(resp)
	if len(hosts) == 0 {
		log.Fatalf("[错误] 无在线主机")
	}

	// ── 选择主机 ──
	var selected hostEntry
	if hostID != "" {
		// -c 指定主机序号，自动匹配
		n := 0
		fmt.Sscanf(hostID, "%d", &n)
		if n < 1 || n > len(hosts) {
			log.Fatalf("[错误] 序号 %d 无效（在线主机: %d台）", n, len(hosts))
		}
		selected = hosts[n-1]
		log.Printf("[自动选择] %s (%s)", selected.name, selected.id)
	} else {
		// 交互选择
		fmt.Printf("\n在线主机:\n")
		fmt.Printf("序号  名称  ID\n")
		for i, h := range hosts {
			fmt.Printf("  %d  %-16s %s\n", i+1, h.name, h.id)
		}
		fmt.Printf("\n请输入序号（5分钟内）: ")
		input := readLineTimeout(5 * time.Minute)
		if input == "" {
			fmt.Println("超时，退出。")
			conn.Close()
			os.Exit(0)
		}
		n := 0
		fmt.Sscanf(input, "%d", &n)
		if n < 1 || n > len(hosts) {
			log.Fatalf("[错误] 序号无效")
		}
		selected = hosts[n-1]
		log.Printf("[选择] %s (%s)", selected.name, selected.id)
	}

	// ── pcinfo + conpc ──
	for {
		if pcinfoAndConnect(conn, selected.id) {
			break
		}
		log.Printf("[连接] 5秒后重试...")
		time.Sleep(5 * time.Second)
	}

	// 忽略 SIGHUP（SSH 断开时终端发的信号，防止进程退出）
	signal.Ignore(syscall.SIGHUP)

	// Ctrl+C / kill
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println()
		log.Printf("[退出] 发送 stopcon<$!$>")
		safeSend(conn, "stopcon<$!$>")
		conn.Close()
		os.Exit(0)
	}()

	// ── 保活 ──
	pcConnected := true
	wsPingInterval := 15 * time.Second // WS 心跳独立于 -t 参数
	wsPingTicker := time.NewTicker(wsPingInterval)
	defer wsPingTicker.Stop()
	hostTicker := time.NewTicker(time.Duration(interval) * time.Second)
	defer hostTicker.Stop()
	log.Printf("[保活] 已连接主机，WS心跳每%ds，主机检查每%ds", int(wsPingInterval.Seconds()), interval)

	for {
		select {
		case <-errCh:
			pcConnected = false
			log.Printf("[保活] WebSocket 断开，重连...")
			conn.Close()
			// 等待router退出，加超时防止死锁
			select {
			case <-routerDone:
			case <-time.After(5 * time.Second):
				log.Printf("[保活] router退出超时，继续重连")
			}
			drainErrCh()
			conn = connect()
			routerDone = startRouter(conn)
			if pcinfoAndConnect(conn, selected.id) {
				pcConnected = true
				drainCh(disconCh)
				log.Printf("[保活] 重连完成")
			} else {
				log.Printf("[保活] 重连失败，等待下次检查")
			}

		case <-disconCh:
			pcConnected = false
			log.Printf("[保活] 远程主机断开，等待下次检查重连...")
			drainCh(disconCh)

		case <-wsPingTicker.C:
			// WS 心跳：发 pclist 文本消息，服务端必回，刷新读超时
			safeSendQuiet(conn, "pclist<$!$>")
			drainCh(pclistCh) // 清空响应，不干扰流程

		case <-hostTicker.C:
			if !pcConnected {
				log.Printf("[保活] 主机断开，重连...")
				if pcinfoAndConnect(conn, selected.id) {
					pcConnected = true
					drainCh(disconCh)
					log.Printf("[保活] 重连完成")
				} else {
					log.Printf("[保活] 重连失败，等待下次检查")
				}
			}
		}
	}
}

// pcinfoAndConnect 发送 pcinfo + conpc，返回连接是否成功
func pcinfoAndConnect(conn *websocket.Conn, id string) bool {
	safeSend(conn, "pcinfo<$!$>"+id)
	if resp, ok := waitMsg(pcinfoCh, 15*time.Second); ok {
		log.Printf("[pcinfo] %s", resp)
	}

	drainCh(conpcCh) // 清空旧响应，防止污染
	drainCh(conerrCh)
	safeSend(conn, "stopcon<$!$>") // 先清除旧连接状态
	time.Sleep(500 * time.Millisecond)
	drainCh(conpcCh)
	drainCh(conerrCh)
	safeSend(conn, "conpc<$!$>"+id)

	// 同时等待 conpc 和 conerr（conpc 最长 120 秒）
	select {
	case resp := <-conpcCh:
		if strings.HasPrefix(resp, "yes") {
			log.Printf("[conpc] 连接成功")
			return true
		}
		parts := strings.SplitN(resp, "<$!$>", 2)
		log.Printf("[conpc] 失败: %s", parts[0])
		return false
	case resp := <-conerrCh:
		log.Printf("[conerr] %s", resp)
		return false
	case <-time.After(120 * time.Second):
		log.Printf("[conpc] 超时无响应")
		return false
	}
}

func drainCh(ch <-chan string) {
	for {
		select {
		case <-ch:
		default:
			return
		}
	}
}

func drainErrCh() {
	for {
		select {
		case <-errCh:
		default:
			return
		}
	}
}

// ─── 连接与消息路由 ───

func connect() *websocket.Conn {
	const (
		initialDelay = time.Second
		maxDelay     = 60 * time.Second
	)
	delay := initialDelay
	for {
		conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
		if err != nil {
			log.Printf("[连接] 失败: %v，%v后重试...", err, delay)
			time.Sleep(delay)
			delay *= 2
			if delay > maxDelay {
				delay = maxDelay
			}
			continue
		}
		// 读超时 5 分钟（服务端可能不支持 WS ping/pong，只靠消息刷新）
		conn.SetReadDeadline(time.Now().Add(5 * time.Minute))
		log.Printf("[连接] 已连接 %s", wsURL)
		return conn
	}
}

func startRouter(conn *websocket.Conn) <-chan struct{} {
	done := make(chan struct{})
	sep := []byte("<$!$>")
	go func() {
		defer close(done)
		for {
			_, raw, err := conn.ReadMessage()
			if err != nil {
				errCh <- err
				log.Printf("[接收] 错误: %v", err)
				return
			}
			// 每收到消息刷新读超时
			conn.SetReadDeadline(time.Now().Add(5 * time.Minute))

			parts := bytes.SplitN(raw, sep, 2)
			msgType := string(parts[0])
			payload := ""
			if len(parts) > 1 {
				payload = string(parts[1])
			}

			// 日志：收到的消息（pclist 频繁且长，不打印）
			if msgType != "pclist" {
				log.Printf("[收到] %s", msgType+payload)
			}

			switch msgType {
			case "pclist":
				pclistCh <- payload
			case "pcinfo":
				pcinfoCh <- payload
			case "conpc":
				conpcCh <- payload
			case "conerr":
				conerrCh <- payload
			case "discon":
				disconCh <- payload
			case "vipend":
				log.Fatalf("[错误] VIP服务已到期，无法连接主机")
			}
		}
	}()
	return done
}

// ─── 工具函数 ───

type hostEntry struct {
	id, name string
}

func hostNames(hosts []hostEntry) string {
	names := make([]string, len(hosts))
	for i, h := range hosts {
		names[i] = h.name
	}
	return strings.Join(names, ", ")
}

func parseHostList(msg string) []hostEntry {
	parts := strings.Split(msg, "<$!$>")
	if len(parts) < 6 {
		return nil
	}
	data := parts[:len(parts)-5]
	var result []hostEntry
	// payload 不含 "pclist" 前缀，从 index 0 开始（script.js 的 msg 含 "pclist" 所以从 1）
	for i := 0; i+3 < len(data); i += 4 {
		id := data[i]
		name := data[i+1]
		if id == "" || id == "me" {
			continue
		}
		result = append(result, hostEntry{id: id, name: name})
	}
	return result
}

func waitMsg(ch <-chan string, timeout time.Duration) (string, bool) {
	select {
	case msg := <-ch:
		return msg, true
	case <-time.After(timeout):
		return "", false
	}
}

func readLineTimeout(timeout time.Duration) string {
	ch := make(chan string, 1)
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		if scanner.Scan() {
			ch <- strings.TrimSpace(scanner.Text())
		}
		close(ch)
	}()
	select {
	case line := <-ch:
		return line
	case <-time.After(timeout):
		return ""
	}
}

func safeSend(conn *websocket.Conn, msg string) {
	safeSendEx(conn, msg, false)
}

// safeSendQuiet 静默发送，不打日志（用于心跳）
func safeSendQuiet(conn *websocket.Conn, msg string) {
	safeSendEx(conn, msg, true)
}

func safeSendEx(conn *websocket.Conn, msg string, quiet bool) {
	if conn == nil {
		return
	}
	conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	err := conn.WriteMessage(websocket.TextMessage, []byte(msg))
	conn.SetWriteDeadline(time.Time{})
	if err != nil {
		log.Printf("[发送] 失败: %v", err)
	} else if !quiet {
		log.Printf("[发送] %s", msg)
	}
}
