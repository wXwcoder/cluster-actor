// Package grains 提供基于 protoactor-go 的虚拟 Actor 定义和实现
package grains

import (
	"fmt"
	"log"
	"sync/atomic"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/cluster-actor/server/gen"
)

// HelloGrain 基于 protoactor-go 的 Hello Actor 实现
type HelloGrain struct {
	BaseGrain
}

func NewHelloGrain() *HelloGrain {
	actor := &HelloGrain{}
	if actor == nil {
		log.Fatalf("Failed to create actor")
	}
	actor.Init()
	return actor
}

// Init 初始化Grain，由集群框架在激活时调用
func (g *HelloGrain) Init() {
	log.Printf("ChatGrain[%s] Init, kind=%s, identity=%s", g.Kind, g.Identity)
}

func (g *HelloGrain) PreStart(ctx actor.Context) {
	log.Printf("HelloGrain[%s] PreStart", ctx.Self().Id)
}

// Receive 处理传入消息，实现 actor.Receiver 接口
func (g *HelloGrain) Receive(ctx actor.Context) {
	g.OnReceive(ctx)
	switch msg := ctx.Message().(type) {
	case *gen.RpcMsg:
		// 处理打招呼请求（远程调用时 proto 消息为指针类型）
		count := atomic.AddInt64(&g.CallCount, 1)
		response := &gen.RpcMsg{
			MsgId:    msg.MsgId,
			Kind:     msg.Kind,
			Data:     []byte(fmt.Sprintf("Hello, %s! 我是Grain[%s], 这是第%d次调用", msg.Name, ctx.Self().Id, count)),
			Identity: ctx.Self().Id,
		}
		ctx.Respond(response)
		log.Printf("HelloGrain[%s] id=%s, RpcMsg, %s! ,第%d次调用，返回 %s", ctx.Self().Id, ctx.Self().Id, msg.Name, count, response.Data)
	case actor.Started:
		// Actor 启动时初始化
		GlobalRegistry.Register(g.Kind, g.Identity)
		log.Printf("HelloGrain[%s] id=%s, 启动, identity=%s", ctx.Self().Id, g.Kind, g.Identity)

	case actor.Stopped:
		GlobalRegistry.Unregister(g.Kind, g.Identity)
		log.Printf("HelloGrain[%s] id=%s, 停止, identity=%s", ctx.Self().Id, g.Kind, g.Identity)
	}
}

// GetCallCount 获取调用次数
func (g *HelloGrain) GetCallCount() int64 {
	return atomic.LoadInt64(&g.CallCount)
}
