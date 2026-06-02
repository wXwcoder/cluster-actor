// Package grains 提供基于 protoactor-go 的虚拟 Actor 定义和实现
package grains

import (
	"context"
	"log"
	"sync/atomic"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/cluster"
	"github.com/bytedance/gopkg/util/logger"
	"github.com/cluster-actor/server/gen"
	"github.com/cluster-actor/server/internal/telemetry"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"
)

type IBaseGrain interface {
	OnReceive(ctx actor.Context) int32
}

type FMsgHandler = func(ctx actor.Context, in *gen.RpcMsg) (proto.Message, gen.ErrorCode)

// BaseGrain 基于 protoactor-go 的虚拟 Actor 实现
type BaseGrain struct {
	PID         *actor.PID
	ActorSystem *actor.ActorSystem
	Kind        string
	Identity    string
	Cluster     *cluster.Cluster
	CallCount   int64
	IsActive    bool
	MsgFunc     map[gen.MsgId]FMsgHandler
}

func (g *BaseGrain) GetKind() string {
	return g.Kind
}

func (g *BaseGrain) GetIdentity() string {
	return g.Identity
}

// SetIdentity 设置 grain 的唯一标识
func (g *BaseGrain) SetIdentity(identity string) {
	g.Identity = identity
}

// SetKind 设置 grain 的类型
func (g *BaseGrain) SetKind(kind string) {
	g.Kind = kind
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

func (g *BaseGrain) onStarted(ctx actor.Context) {
	g.PID = ctx.Self()
	g.ActorSystem = ctx.ActorSystem()

	// 尝试获取 ClusterIdentity（仅当通过 Cluster 创建时有效）
	if extCtx, ok := ctx.(actor.ExtensionContext); ok {
		ci := cluster.GetClusterIdentity(extCtx)
		if ci != nil {
			g.Kind = ci.Kind
			g.Identity = ci.Identity
		}
	}

	// 本地 Actor 使用默认值
	if g.Kind == "" {
		g.Kind = "local_actor"
	}
	if g.Identity == "" {
		g.Identity = ctx.Self().Id
	}

	// 注册到全局 Grain 注册表
	GlobalRegistry.Register(g.Kind, g.Identity)
	log.Printf("BaseGrain started, kind=%s, identity=%s", g.Kind, g.Identity)
	g.IsActive = true
}

func (g *BaseGrain) Init(ctx cluster.GrainContext) {
	g.Cluster = ctx.Cluster()
	log.Printf("BaseGrain[%s] Init", ctx.Self().Id)
}

// func (g *BaseGrain) RequestFuture(identity string, kind string, message interface{}, option ...cluster.GrainCallOption) (actor.Future, error) {
// 	return g.Cluster.RequestFuture(identity, kind, message, option...)
// }

// Receive 处理传入消息，实现 actor.Receiver 接口
// func (g *BaseGrain) Receive(ctx actor.Context) {
// 	//g._receive(g, ctx)
// }

func (g *BaseGrain) Receive(ctx actor.Context) {
	switch msg := ctx.Message().(type) {
	case *actor.Started:
		g.onStarted(ctx)
	case *gen.RpcMsg:
		var msgName string
		reqName, ok := gen.MsgId_name[msg.MsgId]
		if ok {
			msgName = reqName
		}
		log.Printf("BaseActor OnReceive0 identity:%s kind:%s name:%s msgName:%s msgId:%d code:%d traceID:%s",
			g.Identity, g.Kind, msg.Name, reqName, msg.MsgId, msg.Code, msg.TraceID)

		// 使用 OpenTelemetry 创建新的 Span
		tracer := telemetry.GetTracer(g.Kind)
		var spanOpts []trace.SpanStartOption

		// 如果有上游 TraceID，创建 Link 关联分布式追踪链路
		if msg.TraceID != "" {
			if parsedTraceID, err := trace.TraceIDFromHex(msg.TraceID); err == nil {
				// 创建 SpanContext 并构建 Link
				linkCtx := trace.NewSpanContext(trace.SpanContextConfig{
					TraceID: parsedTraceID,
				})
				link := trace.Link{
					SpanContext: linkCtx,
					Attributes: []attribute.KeyValue{
						attribute.String("link.type", "upstream_trace"),
					},
				}
				spanOpts = append(spanOpts, trace.WithLinks(link))
			}
		}

		spanOpts = append(spanOpts, trace.WithAttributes(
			attribute.String("grain.kind", g.Kind),
			attribute.String("grain.identity", g.Identity),
			attribute.String("msg.name", msg.Name),
			attribute.String("msg.type", msgName),
			attribute.Int64("msg.id", int64(msg.MsgId)),
			attribute.String("related_trace_id", msg.TraceID),
		))

		_, span := tracer.Start(context.Background(), "Grain.OnReceive", spanOpts...)
		defer span.End()

		handler, ok := g.MsgFunc[gen.MsgId(msg.MsgId)]
		if ok {
			resp, errCode := handler(ctx, msg)
			if resp != nil {
				msgName = string(resp.ProtoReflect().Descriptor().Name())
				msgId, ok := gen.MsgId_value["P"+msgName]
				if !ok || msgId == 0 {
					msg.Code = int32(gen.ErrorCode_SerializeError)
					span.SetAttributes(attribute.String("error", "unknown_response_msg_id"))
				}
				msg.MsgId = msgId
				b, err := proto.Marshal(resp)
				if err != nil {
					msg.Code = int32(gen.ErrorCode_SerializeError)
					span.SetAttributes(attribute.String("error", "serialize_error"))
				} else {
					msg.Data = b
				}
			}
			msg.Code = int32(errCode)
			span.SetAttributes(attribute.Int("response.code", int(msg.Code)))
		} else {
			msg.Code = int32(gen.ErrorCode_UnknownMsgId)
			span.SetAttributes(attribute.String("error", "unknown_msg_id"))
		}
		ctx.Respond(msg)
		log.Printf("BaseActor OnReceive1 identity:%s kind:%s name:%s msgName:%s msgId:%d code:%d traceID:%s",
			g.Identity, g.Kind, msg.Name, msgName, msg.MsgId, msg.Code, msg.TraceID)
	default:
		log.Printf("BaseActor Receive unknown message type: %T", msg)
	}
}

// func (g *BaseGrain) _receive(grain IBaseGrain, ctx actor.Context) int32 {
// 	return grain.OnReceive(ctx)
// }

// GetCallCount 获取调用次数
func (g *BaseGrain) GetCallCount() int64 {
	return atomic.LoadInt64(&g.CallCount)
}
