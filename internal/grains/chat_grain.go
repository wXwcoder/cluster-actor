// Package grains 提供基于 protoactor-go 的 ChatGrain Actor 实现
package grains

import (
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/cluster"
	"github.com/cluster-actor/server/gen"
	"google.golang.org/protobuf/proto"
)

// ChatGrain 聊天室 Actor 实现
// 负责管理聊天室状态、用户列表和消息广播
type ChatGrain struct {
	BaseGrain
	roomName    string
	creatorId   int64
	creatorName string
	maxMembers  int32
	createdAt   int64
	members     map[int64]*gen.ChatUserInfo // userId -> UserInfo
	messages    []*gen.ChatMessage          // 消息历史记录
	mu          sync.RWMutex                // 保护并发访问
	cluster     *cluster.Cluster
}

func NewChatGrain() *ChatGrain {
	actor := &ChatGrain{BaseGrain: BaseGrain{}}
	if actor == nil {
		log.Fatalf("Failed to create chat grain")
	}
	actor.Init()
	return actor
}

// func (g *ChatGrain) onStarted(ctx actor.Context) {
// 	// Actor 启动时初始化
// 	g.pid = ctx.Self() // 使用 PID 的 ID 作为用户标识
// 	g.actorSystem = ctx.ActorSystem()

// 	GlobalRegistry.Register(g.kind, g.identity)
// 	ctx.Logger().Info("ChatGrain started",
// 		slog.String("pid", g.pid.Id))
// 	log.Printf("ChatGrain[%s] 启动, kind=%s, identity=%s", g.pid.Id, g.kind, g.identity)
// }

// Init 初始化Grain，由集群框架在激活时调用
func (g *ChatGrain) Init() {
	// g.kind = ctx.Kind()
	// g.identity = ctx.Identity()
	// g.roomId = g.identity

	//log.Printf("ChatGrain[%s] Init, kind=%s, identity=%s", ctx.Self().Id, g.kind, g.identity)
	g.members = make(map[int64]*gen.ChatUserInfo)
	g.messages = make([]*gen.ChatMessage, 0, 100)
	g.maxMembers = 50 // 默认最大成员数
	//g.cluster = ctx.Cluster()

	// 注册到全局注册表
	//GlobalRegistry.Register(g.kind, g.identity)

	// 注册Proto消息处理函数
	g.RegisterMsgHandler(gen.MsgId_PChatCreateRoomReq, g.ChatCreateRoomReq)
	g.RegisterMsgHandler(gen.MsgId_PChatJoinRoomReq, g.ChatJoinRoomReq)
	g.RegisterMsgHandler(gen.MsgId_PChatLeaveRoomReq, g.ChatLeaveRoomReq)
	g.RegisterMsgHandler(gen.MsgId_PChatSendMessageReq, g.ChatSendMessageReq)
	g.RegisterMsgHandler(gen.MsgId_PChatGetRoomInfoReq, g.ChatGetRoomInfoReq)
	g.RegisterMsgHandler(gen.MsgId_PChatGetRoomUsersReq, g.ChatGetRoomUsersReq)
	g.RegisterMsgHandler(gen.MsgId_PChatGetMessagesReq, g.ChatGetMessagesReq)
	g.RegisterMsgHandler(gen.MsgId_PChatGetRoomListReq, g.ChatGetRoomListReq)
	log.Printf("ChatGrain[%s] Actor 启动", g.Identity)
}

func (g *ChatGrain) ChatGetRoomListReq(ctx actor.Context, in *gen.RpcMsg) (proto.Message, gen.ErrorCode) {
	var msg gen.ChatGetRoomListReq
	err := proto.Unmarshal(in.Data, &msg)
	if err != nil {
		return nil, gen.ErrorCode_DeSerializeError
	}
	// 处理获取房间列表请求 - 返回全局房间注册表的信息
	allRooms := GlobalRoomRegistry.GetAllRooms()
	total := int32(len(allRooms))
	page := int(msg.GetPage())
	pageSize := int(msg.GetPageSize())

	if page <= 0 {
		page = 1
	}
	if pageSize <= 0 || pageSize > 100 {
		pageSize = 50
	}

	// 分页处理
	startIdx := (page - 1) * pageSize
	if startIdx >= len(allRooms) {
		allRooms = []*gen.ChatRoomInfo{}
	} else {
		endIdx := startIdx + pageSize
		if endIdx > len(allRooms) {
			endIdx = len(allRooms)
		}
		allRooms = allRooms[startIdx:endIdx]
	}

	response := &gen.ChatGetRoomListResp{
		Code:    gen.ErrorCode_OK,
		Message: "成功",
		Rooms:   allRooms,
		Total:   total,
	}
	return response, gen.ErrorCode_OK
}

func (g *ChatGrain) ChatGetMessagesReq(ctx actor.Context, in *gen.RpcMsg) (proto.Message, gen.ErrorCode) {
	var msg gen.ChatGetMessagesReq
	err := proto.Unmarshal(in.Data, &msg)
	if err != nil {
		return nil, gen.ErrorCode_DeSerializeError
	}

	// 处理获取历史消息请求
	g.mu.RLock()
	defer g.mu.RUnlock()

	limit := msg.GetLimit()
	if limit <= 0 || limit > 50 {
		limit = 20
	}

	messages := g.getRecentMessages(int(limit))

	response := &gen.ChatGetMessagesResp{
		Code:     gen.ErrorCode_OK,
		Message:  "成功",
		Messages: messages,
	}
	return response, gen.ErrorCode_OK
}

func (g *ChatGrain) ChatGetRoomUsersReq(ctx actor.Context, in *gen.RpcMsg) (proto.Message, gen.ErrorCode) {
	var msg gen.ChatGetRoomUsersReq
	err := proto.Unmarshal(in.Data, &msg)
	if err != nil {
		return nil, gen.ErrorCode_DeSerializeError
	}

	// 处理获取房间用户列表请求
	g.mu.RLock()
	defer g.mu.RUnlock()

	users := make([]*gen.ChatUserInfo, 0, len(g.members))
	for _, user := range g.members {
		users = append(users, user)
	}

	response := &gen.ChatGetRoomUsersResp{
		Code:    gen.ErrorCode_OK,
		Message: "成功",
		Users:   users,
	}
	return response, gen.ErrorCode_OK
}

func (g *ChatGrain) ChatGetRoomInfoReq(ctx actor.Context, in *gen.RpcMsg) (proto.Message, gen.ErrorCode) {
	var msg gen.ChatGetRoomInfoReq
	err := proto.Unmarshal(in.Data, &msg)
	if err != nil {
		return nil, gen.ErrorCode_DeSerializeError
	}

	// 处理获取房间信息请求
	g.mu.RLock()
	defer g.mu.RUnlock()

	roomInfo := &gen.ChatRoomInfo{
		RoomId:         g.Identity,
		RoomName:       g.roomName,
		CreatorName:    g.creatorName,
		CreatorId:      g.creatorId,
		MaxMembers:     g.maxMembers,
		CurrentMembers: int32(len(g.members)),
		CreatedAt:      g.createdAt,
	}

	response := &gen.ChatGetRoomInfoResp{
		Code:    gen.ErrorCode_OK,
		Message: "成功",
		Room:    roomInfo,
	}
	return response, gen.ErrorCode_OK
}

func (g *ChatGrain) ChatSendMessageReq(ctx actor.Context, in *gen.RpcMsg) (proto.Message, gen.ErrorCode) {
	var msg gen.ChatSendMessageReq
	err := proto.Unmarshal(in.Data, &msg)
	if err != nil {
		return nil, gen.ErrorCode_DeSerializeError
	}
	// 处理发送消息请求
	g.mu.Lock()

	// 检查用户是否在房间中
	if _, exists := g.members[msg.GetSenderId()]; !exists {
		g.mu.Unlock()
		response := &gen.ChatSendMessageResp{
			Code:    gen.ErrorCode_OK,
			Message: "用户不在房间中",
		}
		return response, gen.ErrorCode_OK
	}

	// 创建聊天消息
	chatMsg := &gen.ChatMessage{
		MsgId:      generateMsgId(),
		SenderId:   msg.GetSenderId(),
		SenderName: msg.GetSenderName(),
		RoomId:     g.Identity,
		Type:       msg.GetType(),
		Content:    msg.GetContent(),
		Timestamp:  time.Now().UnixMilli(),
	}

	// 添加到消息历史
	g.messages = append(g.messages, chatMsg)

	// 发送消息给所有用户
	g.broadcastToMembers(ctx, &gen.ChatUserMessage{
		Message: chatMsg,
	}, 0)

	// 保留最近100条消息
	if len(g.messages) > 100 {
		g.messages = g.messages[len(g.messages)-100:]
	}

	g.mu.Unlock()

	log.Printf("ChatGrain[%s] 消息发送: userId=%d, username=%s, content=%s",
		g.Identity, chatMsg.GetSenderId(), chatMsg.GetSenderName(), chatMsg.GetContent())

	// 广播消息给房间内所有用户
	//g.broadcastToMembers(ctx, chatMsg, 0)
	response := &gen.ChatSendMessageResp{
		Code:        gen.ErrorCode_OK,
		Message:     "消息发送成功",
		MessageData: chatMsg,
	}
	return response, gen.ErrorCode_OK
}

func (g *ChatGrain) ChatLeaveRoomReq(ctx actor.Context, in *gen.RpcMsg) (proto.Message, gen.ErrorCode) {
	var msg gen.ChatLeaveRoomReq
	err := proto.Unmarshal(in.Data, &msg)
	if err != nil {
		return nil, gen.ErrorCode_DeSerializeError
	}
	g.mu.Lock()
	defer g.mu.Unlock()

	userId := msg.GetUserId()
	username := msg.GetUsername()

	if _, exists := g.members[userId]; !exists {
		response := &gen.ChatLeaveRoomResp{
			Code:    gen.ErrorCode_OK,
			Message: "用户不在房间中",
		}
		return response, gen.ErrorCode_OK
	}

	// 移除用户
	delete(g.members, userId)

	log.Printf("ChatGrain[%s] 用户离开: userId=%d, username=%s, 剩余人数=%d",
		g.Identity, userId, username, len(g.members))

	// 发送系统消息通知其他用户
	systemMsg := &gen.ChatMessage{
		MsgId:      generateMsgId(),
		SenderId:   0,
		SenderName: "System",
		RoomId:     g.Identity,
		Type:       gen.ChatMsgType_LEAVE,
		Content:    username + " 离开了房间",
		Timestamp:  time.Now().UnixMilli(),
	}
	g.messages = append(g.messages, systemMsg)

	// 广播离开消息给房间内剩余用户
	//g.broadcastToMembers(ctx, systemMsg, 0)

	response := &gen.ChatLeaveRoomResp{
		Code:    gen.ErrorCode_OK,
		Message: "离开房间成功",
	}

	// 更新全局房间注册表成员数
	GlobalRoomRegistry.UpdateRoomMembers(g.Identity, int32(len(g.members)))
	return response, gen.ErrorCode_OK
}

func (g *ChatGrain) ChatJoinRoomReq(ctx actor.Context, in *gen.RpcMsg) (proto.Message, gen.ErrorCode) {
	var msg gen.ChatJoinRoomReq
	err := proto.Unmarshal(in.Data, &msg)
	if err != nil {
		return nil, gen.ErrorCode_DeSerializeError
	}

	// 处理加入房间请求
	g.mu.Lock()
	defer g.mu.Unlock()

	userId := msg.GetUserId()
	username := msg.GetUsername()

	// 检查房间是否已满
	if int32(len(g.members)) >= g.maxMembers {
		response := &gen.ChatJoinRoomResp{
			Code:    gen.ErrorCode_RoomFull,
			Message: "房间已满",
		}
		return response, gen.ErrorCode_RoomFull
	}

	// 检查用户是否已在房间中
	if _, exists := g.members[userId]; exists {
		response := &gen.ChatJoinRoomResp{
			Code:    gen.ErrorCode_OK,
			Message: "用户已在房间中",
			Room: &gen.ChatRoomInfo{
				RoomId:         g.Identity,
				RoomName:       g.roomName,
				CreatorName:    g.creatorName,
				CreatorId:      g.creatorId,
				MaxMembers:     g.maxMembers,
				CurrentMembers: int32(len(g.members)),
				CreatedAt:      g.createdAt,
			},
		}
		return response, gen.ErrorCode_OK
	}

	// 添加用户到房间
	userInfo := &gen.ChatUserInfo{
		UserId:        userId,
		Username:      username,
		IsOnline:      true,
		CurrentRoomId: g.Identity,
		LoginTime:     time.Now().UnixMilli(),
	}
	g.members[userId] = userInfo

	log.Printf("ChatGrain[%s] 用户加入: userId=%d, username=%s, 当前人数=%d",
		g.Identity, userId, username, len(g.members))

	// 发送系统消息通知其他用户
	systemMsg := &gen.ChatMessage{
		MsgId:      generateMsgId(),
		SenderId:   0,
		SenderName: "System",
		RoomId:     g.Identity,
		Type:       gen.ChatMsgType_JOIN,
		Content:    username + " 加入了房间",
		Timestamp:  time.Now().UnixMilli(),
	}
	g.messages = append(g.messages, systemMsg)

	// 广播加入消息给房间内所有用户
	//g.broadcastToMembers(ctx, systemMsg, userId)

	// 获取最近消息
	recentMessages := g.getRecentMessages(20)

	roomInfo := &gen.ChatRoomInfo{
		RoomId:         g.Identity,
		RoomName:       g.roomName,
		CreatorName:    g.creatorName,
		CreatorId:      g.creatorId,
		MaxMembers:     g.maxMembers,
		CurrentMembers: int32(len(g.members)),
		CreatedAt:      g.createdAt,
	}

	response := &gen.ChatJoinRoomResp{
		Code:           gen.ErrorCode_OK,
		Message:        "加入房间成功",
		Room:           roomInfo,
		RecentMessages: recentMessages,
	}
	return response, gen.ErrorCode_OK
}

func (g *ChatGrain) ChatCreateRoomReq(ctx actor.Context, in *gen.RpcMsg) (proto.Message, gen.ErrorCode) {
	var msg gen.ChatCreateRoomReq
	err := proto.Unmarshal(in.Data, &msg)
	if err != nil {
		return nil, gen.ErrorCode_DeSerializeError
	}
	g.mu.Lock()
	g.roomName = msg.GetRoomName()
	g.creatorId = msg.GetCreatorId()
	g.creatorName = msg.GetCreatorName()
	if msg.GetMaxMembers() > 0 {
		g.maxMembers = msg.GetMaxMembers()
	}
	g.createdAt = time.Now().UnixMilli()
	g.mu.Unlock()
	log.Printf("ChatGrain[%s] 房间创建: name=%s, creator=%s, maxMembers=%d",
		g.Identity, g.roomName, g.creatorName, g.maxMembers)
	// 注册到全局房间注册表
	roomInfo := &gen.ChatRoomInfo{
		RoomId:         g.Identity,
		RoomName:       g.roomName,
		CreatorName:    g.creatorName,
		CreatorId:      g.creatorId,
		MaxMembers:     g.maxMembers,
		CurrentMembers: 0,
		CreatedAt:      g.createdAt,
	}

	GlobalRoomRegistry.RegisterRoom(roomInfo)

	log.Printf("ChatGrain[%s] 房间创建: name=%s, creator=%s, maxMembers=%d",
		g.Identity, g.roomName, g.creatorName, g.maxMembers)

	response := &gen.ChatCreateRoomResp{
		Code:    gen.ErrorCode_OK,
		Message: "房间创建成功",
		Room:    roomInfo,
	}
	return response, gen.ErrorCode_OK
}

// Receive 处理传入消息，实现 actor.Receiver 接口
// func (g *ChatGrain) Receive(ctx actor.Context) {
// 	switch ctx.Message().(type) {
// 	case *gen.RpcMsg:
// 		response := g.OnReceive(ctx)
// 		ctx.Respond(response)
// 	//case actor.Started:
// 	case *actor.Started:
// 		g.onStarted(ctx)

// 	case actor.Stopped:
// 		// Actor 停止时清理
// 		GlobalRegistry.Unregister(g.kind, g.identity)
// 		log.Printf("ChatGrain[%s] 停止, identity=%s", g.identity, g.identity)
// 	}
// }

// broadcastToMembers 广播消息给房间内所有用户（排除指定用户）
func (g *ChatGrain) broadcastToMembers(ctx actor.Context, message *gen.ChatUserMessage, excludeUserId int64) {
	// 发布消息到房间
	if err := G.Publish(g.Identity, message); err != nil {
		log.Printf("ChatGrain[%s] 发布消息到房间失败: %v", g.Identity, err)
		return
	}
}

// getRecentMessages 获取最近的消息
func (g *ChatGrain) getRecentMessages(limit int) []*gen.ChatMessage {
	if len(g.messages) <= limit {
		return g.messages
	}
	return g.messages[len(g.messages)-limit:]
}

// getUserIdentity 生成用户Grain的identity
func getUserIdentity(userId int64) string {
	return "user-" + strconv.FormatInt(userId, 10)
}

// generateMsgId 生成唯一消息ID
func generateMsgId() string {
	return time.Now().Format("20060102150405.000")
}
