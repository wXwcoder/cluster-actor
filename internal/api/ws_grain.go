package api

import (
	"log"
	"log/slog"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/cluster-actor/server/gen"
	"github.com/cluster-actor/server/internal/grains"
	"google.golang.org/protobuf/proto"
)

// WebSocketGrain 将 WebSocket 连接封装为本地 Actor
type WebSocketGrain struct {
	grains.BaseGrain
	wc          *WSClient
	topic       string
	userID      string
	actorSystem *actor.ActorSystem
}

func NewWebSocketGrainProps(wc *WSClient, topic string) *actor.Props {
	return actor.PropsFromProducer(func() actor.Actor {
		actor := &WebSocketGrain{
			wc:    wc,
			topic: topic,
		}
		if actor == nil {
			log.Fatalf("Failed to create WebSocket grain")
		}
		actor.Init()
		return actor
	})
}

func (g *WebSocketGrain) Init() {
	g.RegisterMsgHandler(gen.MsgId_PChatUserMessage, g.ChatUserMessage)
	log.Printf("WebSocketGrain[%s] Actor 启动", g.topic)
}

func (g *WebSocketGrain) ChatUserMessage(ctx actor.Context, in *gen.RpcMsg) (proto.Message, gen.ErrorCode) {
	var msg gen.ChatUserMessage
	err := proto.Unmarshal(in.Data, &msg)
	if err != nil {
		return nil, gen.ErrorCode_DeSerializeError
	}

	g.wc.SendToClient(&WSMessage{
		Type:    "new_message",
		Payload: msg.GetMessage(),
	})
	log.Printf("WebSocketGrain[%d] Topic:%s, 接收到消息: %+v", g.userID, g.topic, msg.GetMessage())
	return nil, gen.ErrorCode_OK
}

func (g *WebSocketGrain) Receive(ctx actor.Context) {
	switch msg := ctx.Message().(type) {
	case *gen.RpcMsg:
		response := g.OnReceive(ctx)
		ctx.Respond(response)
	case *actor.Started:
		g.onStarted(ctx)

	case *gen.ChatMessage:
		g.onChatMessage(msg)

	case *actor.Stopping:
		g.onStopping()
	}
}

func (g *WebSocketGrain) onStarted(ctx actor.Context) {
	g.userID = ctx.Self().Id // 使用 PID 的 ID 作为用户标识
	g.actorSystem = ctx.ActorSystem()

	// 启动 WebSocket 读取循环
	go g.readLoop(ctx)

	ctx.Logger().Info("WebSocketGrain started",
		slog.String("user_id", g.userID),
		slog.String("topic", g.topic))
}

func (g *WebSocketGrain) readLoop(ctx actor.Context) {
	// for {
	// 	_, message, err := g.conn.ReadMessage()
	// 	if err != nil {
	// 		ctx.Stop(ctx.Self()) // 连接关闭，停止 Actor
	// 		return
	// 	}

	// 	// 发布消息到房间
	// 	cluster := cluster.GetCluster(g.actorSystem)
	// 	cluster.Publisher().Publish(context.Background(), g.roomID, &gen.ChatMessage{
	// 		SenderId:  g.userID,
	// 		RoomId:    g.roomID,
	// 		Content:   string(message),
	// 		Timestamp: time.Now().UnixMilli(),
	// 	})
	// }
}

func (g *WebSocketGrain) onChatMessage(msg *gen.ChatMessage) {
	// 直接发送到 WebSocket
	// err := g.conn.WriteJSON(msg)
	// if err != nil {
	// 	// 发送失败，断开连接
	// 	g.actorSystem.Root.Stop(g.Self())
	// }
}

func (g *WebSocketGrain) onStopping() {
	g.wc.Close()
}
