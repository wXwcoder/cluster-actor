// Package grains 提供基于 protoactor-go 的虚拟 Actor 定义和实现
package grains

import (
	"log"
	"sync/atomic"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/cluster"
	"github.com/bytedance/gopkg/util/logger"
	"github.com/cluster-actor/server/gen"
	"google.golang.org/protobuf/proto"
)

type IBaseGrain interface {
	OnReceive(ctx actor.Context) int32
}

type FMsgHandler = func(ctx actor.Context, in *gen.RpcMsg) (proto.Message, gen.ErrorCode)

// BaseGrain 基于 protoactor-go 的 Hello Actor 实现
type BaseGrain struct {
	kind      string
	identity  string
	Cluster   *cluster.Cluster
	CallCount int64
	IsActive  bool
	MsgFunc   map[gen.MsgId]FMsgHandler
}

func (g *BaseGrain) GetKind() string {
	return g.kind
}

func (g *BaseGrain) GetIdentity() string {
	return g.identity
}

func (g *BaseGrain) RegisterMsgHandler(messageId gen.MsgId, handler FMsgHandler) {
	if g.MsgFunc == nil {
		g.MsgFunc = make(map[gen.MsgId]FMsgHandler)
	}

	if _, ok := g.MsgFunc[messageId]; !ok {
		g.MsgFunc[messageId] = handler
		logger.Debugf("register messageId: %d", messageId)
	} else if ok {
		logger.Errorf("Duplicate messageId are registered: %d", messageId)
	}
}

func (g *BaseGrain) PreStart(ctx actor.Context) {
	ci := cluster.GetClusterIdentity(ctx)
	if ci != nil {
		g.kind = ci.Kind
		g.identity = ci.Identity
		// 注册到全局Grain注册表
		GlobalRegistry.Register(g.kind, g.identity)
		log.Printf("BaseGrain[%s] id=%s, RpcReq, identity=%s", ctx.Self().Id, g.kind, g.identity)
	} else {
		log.Printf("BaseGrain[%s] id=%s, RpcReq, 但未获取到ClusterIdentity", ctx.Self().Id, g.kind)
	}
	g.IsActive = true
}

func (g *BaseGrain) Init(ctx cluster.GrainContext) {
	g.Cluster = ctx.Cluster()
	log.Printf("BaseGrain[%s] Init", ctx.Self().Id)
}

func (g *BaseGrain) RequestFuture(identity string, kind string, message interface{}, option ...cluster.GrainCallOption) (actor.Future, error) {
	return g.Cluster.RequestFuture(identity, kind, message, option...)
}

// Receive 处理传入消息，实现 actor.Receiver 接口
// func (g *BaseGrain) Receive(ctx actor.Context) {
// 	//g._receive(g, ctx)
// }

func (g *BaseGrain) OnReceive(ctx actor.Context) *gen.RpcMsg {
	if !g.IsActive {
		g.PreStart(ctx)
	}
	switch msg := ctx.Message().(type) {
	case *gen.RpcMsg:
		var msgName string
		reqName, ok := gen.MsgId_name[msg.MsgId]
		if ok {
			msgName = reqName
		}
		log.Printf("BaseActor OnReceive0 identity:%s kind:%s name:%s msgName:%s msgId:%d code:%d", g.identity, g.kind, msg.Name, reqName, msg.MsgId, msg.Code)
		handler, ok := g.MsgFunc[gen.MsgId(msg.MsgId)]
		if ok {
			resp, errCode := handler(ctx, msg)
			if resp != nil {
				msgName = string(resp.ProtoReflect().Descriptor().Name())
				msgId, ok := gen.MsgId_value["P"+msgName]
				if !ok || msgId == 0 {
					msg.Code = int32(gen.ErrorCode_SerializeError)
				}
				msg.MsgId = msgId
				b, err := proto.Marshal(resp)
				if err != nil {
					msg.Code = int32(gen.ErrorCode_SerializeError)
				} else {
					msg.Data = b
				}
			}
			msg.Code = int32(errCode)
		} else {
			msg.Code = int32(gen.ErrorCode_UnknownMsgId)
		}
		log.Printf("BaseActor OnReceive1 identity:%s kind:%s name:%s msgName:%s msgId:%d code:%d", g.identity, g.kind, msg.Name, msgName, msg.MsgId, msg.Code)
		return msg
	default:
		return &gen.RpcMsg{
			Kind:     g.kind,
			Identity: g.identity,
			Code:     int32(gen.ErrorCode_UnknownMsgId),
		}
	}
}

// func (g *BaseGrain) _receive(grain IBaseGrain, ctx actor.Context) int32 {
// 	return grain.OnReceive(ctx)
// }

// GetCallCount 获取调用次数
func (g *BaseGrain) GetCallCount() int64 {
	return atomic.LoadInt64(&g.CallCount)
}
