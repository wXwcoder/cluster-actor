package api

import (
	"encoding/json"
	"log"
	"sync"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/cluster"
	"github.com/cluster-actor/server/gen"
	"github.com/gorilla/websocket"
)

type SubscribeHandler func(ctx actor.Context)

// WSClient WebSocket客户端连接
type WSClient struct {
	cluster     *cluster.Cluster
	conn        *websocket.Conn
	userID      int64
	username    string
	sessionID   string
	currentRoom string
	send        chan []byte
	mu          sync.Mutex
}

func NewWSClient(cluster *cluster.Cluster, conn *websocket.Conn) *WSClient {
	return &WSClient{
		cluster: cluster,
		conn:    conn,
		send:    make(chan []byte, 256),
	}
}

func (wc *WSClient) SubscribeTopic(topic string) {
	// 1. 在本地创建 WebSocketGrain（关键！）
	// 使用 actor.Spawn 而不是 cluster.Spawn
	props := NewWebSocketGrainProps(wc, topic)
	pid := wc.cluster.ActorSystem.Root.Spawn(props) // 本地创建

	// 2. 本地订阅（关键！）
	// 使用 SubscribeByPid 确保只在本地订阅
	_, err := wc.cluster.SubscribeByPid(topic, pid)
	if err != nil {
		wc.cluster.ActorSystem.Root.Stop(pid)
		// 处理订阅错误
		log.Printf("SubscribeTopic failed: %v", err)
		return
	}
	log.Printf("WSClient[%d:%s] SubscribeTopic success: %s", wc.userID, wc.username, topic)
}

func (wc *WSClient) handleRpcMsg(msg *gen.RpcMsg) {
	// 处理消息逻辑
	log.Printf("RpcMsg, %s, %s, %s, %s", msg.Kind, msg.Name, msg.Data, msg.Identity)

}

func (wm *WSClient) SendToClient(msg *WSMessage) {
	data, err := json.Marshal(msg)
	if err != nil {
		log.Printf("JSON序列化错误: %v", err)
		return
	}
	log.Printf("WSClient[%d:%s]: %s", wm.userID, wm.username, string(data))
	select {
	case wm.send <- data:
	default:
		close(wm.send)
	}
}

func (wc *WSClient) Close() {
	wc.conn.Close()
}
