package api

import (
	"fmt"
	"log"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/cluster-actor/server/gen"
	"github.com/cluster-actor/server/internal/grains"
	"github.com/cluster-actor/server/pkg/types"
	"google.golang.org/protobuf/proto"
)

// WebSocketGrain 将 WebSocket 连接封装为本地 Actor
type WebSocketGrain struct {
	grains.BaseGrain
	wc          *WSClient
	topic       string
	userID      int64
	actorSystem *actor.ActorSystem
}

func NewWebSocketGrainProps(wc *WSClient, topic string) *actor.Props {
	return actor.PropsFromProducer(func() actor.Actor {
		grain := &WebSocketGrain{
			wc:    wc,
			topic: topic,
		}
		// 设置 WebSocketGrain 的类型
		grain.Kind = string(types.Kind_WsClient)
		grain.Identity = fmt.Sprintf("%d", wc.userID) // 使用 sessionID
		//grain.UserID = topic       // 使用 topic 作为用户标识

		if grain == nil {
			log.Fatalf("Failed to create WebSocket grain")
		}
		grain.Init()
		return grain
	})
}

func (g *WebSocketGrain) Init() {
	g.RegisterMsgHandler(gen.MsgId_PChatUserMessage, g.ChatUserMessage)
	log.Printf("WebSocketGrain[%s] Actor 启动", g.topic)
}

// func (g *WebSocketGrain) Receive(ctx actor.Context) {
// 	switch msg := ctx.Message().(type) {
// 	case *actor.Started:
// 		g.actorSystem = ctx.ActorSystem()
// 		// 启动 WebSocket 读取循环
// 		go g.readLoop(ctx)

// 		ctx.Logger().Info("WebSocketGrain started",
// 			slog.String("user_id", g.userID),
// 			slog.String("topic", g.topic),
// 			slog.String("kind", g.Kind),
// 			slog.String("identity", g.Identity))
// 	case *actor.Stopping:
// 		g.onStopping()
// 	default:
// 		g.BaseGrain.Receive(ctx)
// 	}
// }

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

func (g *WebSocketGrain) readLoop(ctx actor.Context) {
	// TODO: WebSocket 读取循环实现
}

func (g *WebSocketGrain) onStopping() {
	g.wc.Close()
}
