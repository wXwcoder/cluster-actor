// Package grains 提供基于 protoactor-go 的虚拟 Actor 定义和实现
package grains

import (
	"log"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/cluster"
	"github.com/cluster-actor/server/gen"
	"github.com/cluster-actor/server/pkg/types"
	"google.golang.org/protobuf/proto"
)

// SessionManager 用于管理WebSocket会话的接口
type SessionManager interface {
	SendMessage(sessionID string, message interface{}) error
	RemoveSession(userID int64)
}

// UserGrain 基于 protoactor-go 的 User Actor 实现
// 负责管理用户身份信息和登录状态
type UserGrain struct {
	BaseGrain
	kind        string
	identity    string
	userId      int64
	username    string
	sessionId   string
	isOnline    bool
	currentRoom string
	loginTime   int64
	cluster     *cluster.Cluster
}

func NewUserGrain() *UserGrain {
	actor := &UserGrain{}
	if actor == nil {
		log.Fatalf("Failed to create actor")
	}
	actor.Init()
	return actor
}

// Init 初始化Grain，由集群框架在激活时调用
func (g *UserGrain) Init() {
	// g.kind = ctx.Kind()
	// g.identity = ctx.Identity()

	// log.Printf("UserGrain[%s] Init, kind=%s, identity=%s", ctx.Self().Id, g.kind, g.identity)
	// g.cluster = ctx.Cluster()

	// // 注册到全局注册表
	// GlobalRegistry.Register(g.kind, g.identity)

	g.RegisterMsgHandler(gen.MsgId_PChatLoginReq, g.ChatLoginReq)
	g.RegisterMsgHandler(gen.MsgId_PChatLogoutReq, g.ChatLogoutReq)
	g.RegisterMsgHandler(gen.MsgId_PChatUserMessage, g.ChatUserMessage)
	g.RegisterMsgHandler(gen.MsgId_PChatGetUserInfoReq, g.ChatGetUserInfoReq)

	log.Printf("UserGrain[%s] Actor 启动", g.identity)
}

func (g *UserGrain) ChatGetUserInfoReq(ctx actor.Context, in *gen.RpcMsg) (proto.Message, gen.ErrorCode) {
	var msg gen.ChatGetUserInfoReq
	err := proto.Unmarshal(in.Data, &msg)
	if err != nil {
		return nil, gen.ErrorCode_DeSerializeError
	}

	// 处理获取用户信息请求
	userInfo := &gen.ChatUserInfo{
		UserId:        g.userId,
		Username:      g.username,
		IsOnline:      g.isOnline,
		CurrentRoomId: g.currentRoom,
		LoginTime:     g.loginTime,
	}

	response := &gen.ChatGetUserInfoResp{
		Code:    gen.ErrorCode_OK,
		Message: "成功",
		User:    userInfo,
	}
	return response, gen.ErrorCode_OK
}

func (g *UserGrain) ChatUserMessage(ctx actor.Context, in *gen.RpcMsg) (proto.Message, gen.ErrorCode) {
	var msg gen.ChatUserMessage
	err := proto.Unmarshal(in.Data, &msg)
	if err != nil {
		return nil, gen.ErrorCode_DeSerializeError
	}
	// 接收发送给用户的消息（从ChatGrain转发过来）
	if !g.isOnline {
		log.Printf("UserGrain[%s] 用户不在线，无法接收消息", g.identity)
		return nil, gen.ErrorCode_UserNotLoggedIn
	}

	log.Printf("UserGrain[%s] 接收到消息: %+v", g.identity, msg.GetMessage())

	// TODO: 通过WebSocket会话管理器发送消息到客户端
	// if g.sessionId != "" {
	//     sessionManager.SendMessage(g.sessionId, msg.GetMessage())
	// }
	return &msg, gen.ErrorCode_OK
}

func (g *UserGrain) ChatLogoutReq(ctx actor.Context, in *gen.RpcMsg) (proto.Message, gen.ErrorCode) {
	var msg gen.ChatLogoutReq
	err := proto.Unmarshal(in.Data, &msg)
	if err != nil {
		return nil, gen.ErrorCode_DeSerializeError
	}
	// 处理登出请求

	g.isOnline = false
	g.sessionId = ""

	// 如果用户在房间中，需要离开房间
	if g.currentRoom != "" {
		g.leaveCurrentRoom(ctx)
	}

	response := &gen.ChatLogoutResp{
		Code:    gen.ErrorCode_OK,
		Message: "登出成功",
	}
	log.Printf("UserGrain[%s] 用户登出: userId=%d", g.identity, g.userId)
	return response, gen.ErrorCode_OK
}

func (g *UserGrain) ChatLoginReq(ctx actor.Context, in *gen.RpcMsg) (proto.Message, gen.ErrorCode) {
	var msg gen.ChatLoginReq
	err := proto.Unmarshal(in.Data, &msg)
	if err != nil {
		return nil, gen.ErrorCode_DeSerializeError
	}
	g.userId = msg.GetUserId()
	g.username = msg.GetUsername()
	g.sessionId = msg.GetSessionId()
	g.isOnline = true
	g.loginTime = time.Now().UnixMilli()

	log.Printf("UserGrain[%s] 用户登录: userId=%d, username=%s, sessionId=%s",
		g.identity, g.userId, g.username, g.sessionId)

	userInfo := &gen.ChatUserInfo{
		UserId:        g.userId,
		Username:      g.username,
		IsOnline:      g.isOnline,
		CurrentRoomId: g.currentRoom,
		LoginTime:     g.loginTime,
	}

	response := &gen.ChatLoginResp{
		Code:    gen.ErrorCode_OK,
		Message: "登录成功",
		User:    userInfo,
	}
	return response, gen.ErrorCode_OK
}

// Receive 处理传入消息，实现 actor.Receiver 接口
func (g *UserGrain) Receive(ctx actor.Context) {
	switch ctx.Message().(type) {
	case *gen.RpcMsg:
		response := g.OnReceive(ctx)
		ctx.Respond(response)
	case actor.Started:
		// Actor 启动时初始化
		GlobalRegistry.Register(g.kind, g.identity)
		log.Printf("UserGrain[%s] 启动, identity=%s", g.identity, g.identity)

	case actor.Stopped:
		// Actor 停止时清理
		if g.isOnline && g.currentRoom != "" {
			g.leaveCurrentRoom(ctx)
		}
		GlobalRegistry.Unregister(g.kind, g.identity)
		log.Printf("UserGrain[%s] 停止, identity=%s", g.identity, g.identity)
	}
}

// leaveCurrentRoom 离开当前房间
func (g *UserGrain) leaveCurrentRoom(ctx actor.Context) {
	if g.currentRoom == "" {
		return
	}

	log.Printf("UserGrain[%s] 离开房间: %s", g.identity, g.currentRoom)

	// 发送离开请求到ChatGrain
	leaveReq := &gen.ChatLeaveRoomReq{
		RoomId:   g.currentRoom,
		UserId:   g.userId,
		Username: g.username,
	}
	g.cluster.RequestFuture(g.currentRoom, string(types.Kind_Chat), leaveReq)

	g.currentRoom = ""
}
