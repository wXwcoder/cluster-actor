// Package grains 提供基于 protoactor-go 的虚拟 Actor 定义和实现
package grains

import (
	"fmt"
	"log"
	"sync/atomic"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/cluster"
	"github.com/cluster-actor/server/gen"
)

// HelloGrain 基于 protoactor-go 的 Hello Actor 实现
type HelloGrain struct {
	kind      string
	identity  string
	callCount int64
}

func (g *HelloGrain) PreStart(ctx actor.Context) {
	log.Printf("HelloGrain[%s] PreStart", ctx.Self().Id)
}

func (h *HelloGrain) Init(ctx cluster.GrainContext) {
	log.Printf("HelloGrain[%s] Init", ctx.Self().Id)
}

// Receive 处理传入消息，实现 actor.Receiver 接口
func (g *HelloGrain) Receive(ctx actor.Context) {
	switch msg := ctx.Message().(type) {
	case *gen.RpcReq:
		ci := cluster.GetClusterIdentity(ctx)
		if ci != nil {
			g.kind = ci.Kind
			g.identity = ci.Identity
			// 注册到全局Grain注册表
			GlobalRegistry.Register(g.kind, g.identity)
			log.Printf("HelloGrain[%s] id=%s, RpcReq, identity=%s", ctx.Self().Id, g.kind, g.identity)
		} else {
			log.Printf("HelloGrain[%s] id=%s, RpcReq, 但未获取到ClusterIdentity, MsgId=%d, Data=%s", ctx.Self().Id, g.kind, msg.MsgId, msg.Data)
		}
		// 处理打招呼请求（远程调用时 proto 消息为指针类型）
		count := atomic.AddInt64(&g.callCount, 1)
		response := &gen.RpcResp{
			Message:   fmt.Sprintf("Hello, %s! 我是Grain[%s], 这是第%d次调用", msg.Name, ctx.Self().Id, count),
			CallCount: count,
			Identity:  ctx.Self().Id,
		}
		ctx.Respond(response)
		log.Printf("HelloGrain[%s] id=%s, RpcResp, %s! ,第%d次调用，返回 %s", ctx.Self().Id, ctx.Self().Id, msg.Name, count, response.Message)
	case actor.Started:
		// Actor 启动时初始化
		GlobalRegistry.Register(g.kind, g.identity)
		log.Printf("HelloGrain[%s] id=%s, 启动, identity=%s", ctx.Self().Id, g.kind, g.identity)

	case actor.Stopped:
		GlobalRegistry.Unregister(g.kind, g.identity)
		log.Printf("HelloGrain[%s] id=%s, 停止, identity=%s", ctx.Self().Id, g.kind, g.identity)
	}
}

// GetCallCount 获取调用次数
func (g *HelloGrain) GetCallCount() int64 {
	return atomic.LoadInt64(&g.callCount)
}
