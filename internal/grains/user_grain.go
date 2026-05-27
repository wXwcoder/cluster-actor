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

// UserGrain 基于 protoactor-go 的 User Actor 实现
type UserGrain struct {
	kind      string
	identity  string
	userId    int64
	callCount int64
}

// Receive 处理传入消息，实现 actor.Receiver 接口
func (g *UserGrain) Receive(ctx actor.Context) {
	switch msg := ctx.Message().(type) {
	case *gen.RpcReq:
		ci := cluster.GetClusterIdentity(ctx)
		if ci != nil {
			g.kind = ci.Kind
			g.identity = ci.Identity
			// 注册到全局Grain注册表
			GlobalRegistry.Register(g.kind, g.identity)
			log.Printf("UserGrain[%s] id=%s, RpcReq, identity=%s, MsgId=%d, Data=%s", ctx.Self().Id, g.kind, g.identity, msg.MsgId, msg.Data)
		} else {
			log.Printf("UserGrain[%s] id=%s, RpcReq, 但未获取到ClusterIdentity, MsgId=%d, Data=%s", ctx.Self().Id, g.kind, msg.MsgId, msg.Data)	
		}
		// 处理打招呼请求（远程调用时 proto 消息为指针类型）
		count := atomic.AddInt64(&g.callCount, 1)
		response := &gen.RpcResp{
			Message:   fmt.Sprintf("Hello, %s! [%s], 这是第%d次调用", msg.Name, ctx.Self().Id, count),
			CallCount: count,
			Identity:  ctx.Self().Id,
		}
		ctx.Respond(response)
		log.Printf("UserGrain[%s] id=%s, %s! ,第%d次调用，返回 %s", ctx.Self().Id, g.kind, msg.Name, count, response.Message)
	case *gen.LoginReq:
		// 处理登录请求（远程调用时 proto 消息为指针类型）
		count := atomic.AddInt64(&g.callCount, 1)
		response := &gen.LoginResp{
			Success: true,
			UserId:  count,
		}
		ctx.Respond(response)
		log.Printf("UserGrain[%s] , %s! ,第%d次调用，返回 %d", ctx.Self().Id, msg.Username, count, response.UserId)
	case actor.Started:
		// Actor 启动时初始化
		GlobalRegistry.Register(g.kind, g.identity)
		log.Printf("UserGrain[%s] id=%s, 启动, identity=%s", ctx.Self().Id, g.kind, g.identity)

	case actor.Stopped:
		// Actor 停止时清理
		GlobalRegistry.Unregister(g.kind, g.identity)
		log.Printf("UserGrain[%s] kind=%s, 停止, identity=%s", ctx.Self().Id, g.kind, g.identity)
	}
}

// GetCallCount 获取调用次数
func (g *UserGrain) GetCallCount() int64 {
	return atomic.LoadInt64(&g.callCount)
}
