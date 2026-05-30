// Package api 提供WebSocket连接管理和消息处理
package api

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/asynkron/protoactor-go/cluster"
	"github.com/cluster-actor/server/gen"
	"github.com/cluster-actor/server/internal/global"
	"github.com/cluster-actor/server/internal/grains"
	"github.com/cluster-actor/server/pkg/types"
	"github.com/cluster-actor/server/pkg/utils"
	"github.com/gorilla/websocket"
)

// WSMessage WebSocket消息结构
type WSMessage struct {
	Type    string      `json:"type"`
	Payload interface{} `json:"payload,omitempty"`
}

// WebSocketManager WebSocket连接管理器
type WebSocketManager struct {
	cluster   *cluster.Cluster
	clients   map[int64]*WSClient // userID -> WSClient
	clientsMu sync.RWMutex
	upgrader  websocket.Upgrader
}

// NewWebSocketManager 创建WebSocket管理器
func NewWebSocketManager(cluster *cluster.Cluster) *WebSocketManager {
	return &WebSocketManager{
		cluster: cluster,
		clients: make(map[int64]*WSClient),
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 1024,
			CheckOrigin: func(r *http.Request) bool {
				return true
			},
		},
	}
}

// HandleWebSocket 处理WebSocket连接
func (wm *WebSocketManager) HandleWebSocket(w http.ResponseWriter, r *http.Request) {
	conn, err := wm.upgrader.Upgrade(w, r, nil)
	if err != nil {
		log.Printf("WebSocket升级失败: %v", err)
		return
	}

	client := NewWSClient(wm.cluster, conn)

	go wm.readPump(client)
	go wm.writePump(client)
}

// readPump 从WebSocket读取消息
func (wm *WebSocketManager) readPump(client *WSClient) {
	defer func() {
		wm.unregisterClient(client)
		client.conn.Close()
	}()

	client.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
	client.conn.SetPongHandler(func(string) error {
		client.conn.SetReadDeadline(time.Now().Add(60 * time.Second))
		return nil
	})

	for {
		_, message, err := client.conn.ReadMessage()
		if err != nil {
			if websocket.IsUnexpectedCloseError(err, websocket.CloseGoingAway, websocket.CloseNormalClosure) {
				log.Printf("WebSocket读取错误: %v", err)
			}
			break
		}

		var wsMsg WSMessage
		if err := json.Unmarshal(message, &wsMsg); err != nil {
			log.Printf("JSON解析错误: %v", err)
			continue
		}

		wm.handleMessage(client, &wsMsg)
	}
}

// writePump 向WebSocket写入消息
func (wm *WebSocketManager) writePump(client *WSClient) {
	ticker := time.NewTicker(30 * time.Second)
	defer func() {
		ticker.Stop()
		client.conn.Close()
	}()

	for {
		select {
		case message, ok := <-client.send:
			client.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if !ok {
				client.conn.WriteMessage(websocket.CloseMessage, []byte{})
				return
			}

			w, err := client.conn.NextWriter(websocket.TextMessage)
			if err != nil {
				return
			}
			w.Write(message)

			n := len(client.send)
			for i := 0; i < n; i++ {
				w.Write([]byte{'\n'})
				w.Write(<-client.send)
			}

			if err := w.Close(); err != nil {
				return
			}
		case <-ticker.C:
			client.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			if err := client.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

// handleMessage 处理WebSocket消息
func (wm *WebSocketManager) handleMessage(client *WSClient, msg *WSMessage) {
	switch msg.Type {
	case "login":
		wm.handleLogin(client, msg)
	case "logout":
		wm.handleLogout(client)
	case "create_room":
		wm.handleCreateRoom(client, msg)
	case "join_room":
		wm.handleJoinRoom(client, msg)
	case "leave_room":
		wm.handleLeaveRoom(client)
	case "send_message":
		wm.handleSendMessage(client, msg)
	case "get_room_list":
		wm.handleGetRoomList(client, msg)
	case "get_room_users":
		wm.handleGetRoomUsers(client, msg)
	default:
		wm.sendToClient(client, &WSMessage{
			Type:    "error",
			Payload: map[string]string{"message": "未知消息类型"},
		})
	}
}

// handleLogin 处理登录消息
func (wm *WebSocketManager) handleLogin(client *WSClient, msg *WSMessage) {
	payload, ok := msg.Payload.(map[string]interface{})
	if !ok {
		wm.sendToClient(client, &WSMessage{
			Type:    "login_error",
			Payload: map[string]string{"message": "无效的消息格式"},
		})
		return
	}

	userID := int64(payload["user_id"].(float64))
	username := payload["username"].(string)
	sessionID := payload["session_id"].(string)

	// 检查用户是否已登录
	wm.clientsMu.RLock()
	if _, exists := wm.clients[userID]; exists {
		wm.clientsMu.RUnlock()
		wm.sendToClient(client, &WSMessage{
			Type:    "login_error",
			Payload: map[string]string{"message": "用户已在线"},
		})
		return
	}
	wm.clientsMu.RUnlock()

	// 注册客户端
	client.userID = userID
	client.username = username
	client.sessionID = sessionID

	wm.clientsMu.Lock()
	wm.clients[userID] = client
	wm.clientsMu.Unlock()

	// 通知UserGrain登录
	userIdentity := fmt.Sprintf("user-%d", userID)
	loginReq := &gen.ChatLoginReq{
		UserId:    userID,
		Username:  username,
		SessionId: sessionID,
	}

	resp, err := global.Rpc(userIdentity, string(types.Kind_User), "Login", loginReq)
	if err != nil || resp == nil {
		log.Printf("登录请求失败: %v", err)
		wm.sendToClient(client, &WSMessage{
			Type:    "login_error",
			Payload: map[string]string{"message": "登录失败"},
		})
		return
	}

	// future, err := wm.cluster.RequestFuture(userIdentity, string(types.Kind_User), loginReq)
	// if err != nil {
	// 	log.Printf("登录请求失败: %v", err)
	// 	wm.sendToClient(client, &WSMessage{
	// 		Type:    "login_error",
	// 		Payload: map[string]string{"message": "登录失败"},
	// 	})
	// 	return
	// }
	// result, err := future.Result()
	// if err != nil {
	// 	log.Printf("登录失败: %v", err)
	// 	wm.sendToClient(client, &WSMessage{
	// 		Type:    "login_error",
	// 		Payload: map[string]string{"message": "登录失败"},
	// 	})
	// 	return
	// }

	log.Printf("登录响应: userID:%d %v", userID, resp)
	if resp, ok := resp.(*gen.ChatLoginResp); ok {
		if resp.GetCode() == gen.ErrorCode_OK {
			// 登录成功，返回用户信息
			wm.sendToClient(client, &WSMessage{
				Type:    "login_success",
				Payload: resp.GetUser(),
			})
		} else {
			wm.sendToClient(client, &WSMessage{
				Type:    "login_error",
				Payload: map[string]string{"message": resp.GetMessage()},
			})
		}
	}
}

// handleLogout 处理登出消息
func (wm *WebSocketManager) handleLogout(client *WSClient) {
	if client.userID == 0 {
		return
	}

	userIdentity := "user-" + strconv.FormatInt(client.userID, 10)
	logoutReq := &gen.ChatLogoutReq{
		UserId:    client.userID,
		SessionId: client.sessionID,
	}

	wm.cluster.RequestFuture(userIdentity, string(types.Kind_User), logoutReq)

	resp, err := global.Rpc(userIdentity, string(types.Kind_User), "Logout", logoutReq)
	if err != nil || resp == nil {
		log.Printf("登出请求失败: %v", err)
		wm.sendToClient(client, &WSMessage{
			Type:    "error",
			Payload: map[string]string{"message": "登出失败"},
		})
		return
	}

	if resp, ok := resp.(*gen.ChatLogoutResp); ok {
		if resp.GetCode() == gen.ErrorCode_OK {
			wm.unregisterClient(client)
			wm.sendToClient(client, &WSMessage{
				Type:    "logout_success",
				Payload: map[string]string{"message": "登出成功"},
			})
		} else {
			wm.sendToClient(client, &WSMessage{
				Type:    "error",
				Payload: map[string]string{"message": resp.GetMessage()},
			})
		}
		return
	}

	wm.sendToClient(client, &WSMessage{
		Type:    "error",
		Payload: map[string]string{"message": "登出失败"},
	})
}

// handleCreateRoom 处理创建房间消息
func (wm *WebSocketManager) handleCreateRoom(client *WSClient, msg *WSMessage) {
	if client.userID == 0 {
		wm.sendToClient(client, &WSMessage{
			Type:    "error",
			Payload: map[string]string{"message": "请先登录"},
		})
		return
	}

	payload, ok := msg.Payload.(map[string]interface{})
	if !ok {
		wm.sendToClient(client, &WSMessage{
			Type:    "error",
			Payload: map[string]string{"message": "无效的消息格式"},
		})
		return
	}

	roomName := payload["room_name"].(string)
	if roomName == "" {
		roomName = "room-" + utils.Ulid()
	}
	maxMembers := int32(payload["max_members"].(float64))

	roomIdentity := roomName
	createReq := &gen.ChatCreateRoomReq{
		RoomName:    roomName,
		CreatorId:   client.userID,
		CreatorName: client.username,
		MaxMembers:  maxMembers,
	}

	resp, err := global.Rpc(roomIdentity, string(types.Kind_Chat), "CreateRoom", createReq)
	if err != nil || resp == nil {
		log.Printf("创建房间请求失败: %v", err)
		wm.sendToClient(client, &WSMessage{
			Type:    "error",
			Payload: map[string]string{"message": "创建房间失败"},
		})
		return
	}

	if resp, ok := resp.(*gen.ChatCreateRoomResp); ok {
		if resp.GetCode() == gen.ErrorCode_OK {
			wm.sendToClient(client, &WSMessage{
				Type:    "room_created",
				Payload: map[string]string{"message": resp.GetMessage()},
			})
		} else {
			wm.sendToClient(client, &WSMessage{
				Type:    "error",
				Payload: map[string]string{"message": resp.GetMessage()},
			})
		}
		return
	}

	wm.sendToClient(client, &WSMessage{
		Type:    "error",
		Payload: map[string]string{"message": "创建房间失败"},
	})
}

// handleJoinRoom 处理加入房间消息
func (wm *WebSocketManager) handleJoinRoom(client *WSClient, msg *WSMessage) {
	if client.userID == 0 {
		wm.sendToClient(client, &WSMessage{
			Type:    "error",
			Payload: map[string]string{"message": "请先登录"},
		})
		return
	}

	payload, ok := msg.Payload.(map[string]interface{})
	if !ok {
		wm.sendToClient(client, &WSMessage{
			Type:    "error",
			Payload: map[string]string{"message": "无效的消息格式"},
		})
		return
	}

	roomID := payload["room_id"].(string)

	joinReq := &gen.ChatJoinRoomReq{
		RoomId:   roomID,
		UserId:   client.userID,
		Username: client.username,
	}

	//future, err := wm.cluster.RequestFuture(roomID, string(types.Kind_Chat), joinReq)
	resp, err := global.Rpc(roomID, string(types.Kind_Chat), "JoinRoom", joinReq)
	if err != nil || resp == nil {
		log.Printf("加入房间请求失败: %v", err)
		wm.sendToClient(client, &WSMessage{
			Type:    "error",
			Payload: map[string]string{"message": "加入房间失败"},
		})
		return
	}

	if resp, ok := resp.(*gen.ChatJoinRoomResp); ok {
		if resp.GetCode() == gen.ErrorCode_OK {
			// 加入房间成功, 订阅当前房间
			client.currentRoom = roomID
			client.SubscribeTopic(roomID)
			wm.sendToClient(client, &WSMessage{
				Type: "room_joined",
				Payload: map[string]interface{}{
					"room": resp.GetRoom(),
				},
			})
		} else {
			log.Printf("加入房间失败: %v", err)
			wm.sendToClient(client, &WSMessage{
				Type:    "room_joined",
				Payload: map[string]string{"message": "加入房间失败"},
			})
		}
	}
}

// handleLeaveRoom 处理离开房间消息
func (wm *WebSocketManager) handleLeaveRoom(client *WSClient) {
	if client.userID == 0 || client.currentRoom == "" {
		return
	}

	leaveReq := &gen.ChatLeaveRoomReq{
		RoomId:   client.currentRoom,
		UserId:   client.userID,
		Username: client.username,
	}

	resp, err := global.Rpc(client.currentRoom, string(types.Kind_Chat), "LeaveRoom", leaveReq)
	if resp, ok := resp.(*gen.ChatLeaveRoomResp); ok {
		if resp.GetCode() == gen.ErrorCode_OK {
			client.currentRoom = ""
			wm.sendToClient(client, &WSMessage{
				Type:    "room_left",
				Payload: map[string]string{"message": resp.GetMessage()},
			})
		} else {
			log.Printf("离开房间失败: %v", err)
		}
		return
	}

	wm.sendToClient(client, &WSMessage{
		Type:    "room_left",
		Payload: map[string]string{"message": "离开房间失败"},
	})
}

// handleSendMessage 处理发送消息
func (wm *WebSocketManager) handleSendMessage(client *WSClient, msg *WSMessage) {
	if client.userID == 0 || client.currentRoom == "" {
		wm.sendToClient(client, &WSMessage{
			Type:    "error",
			Payload: map[string]string{"message": "请先加入房间"},
		})
		return
	}

	payload, ok := msg.Payload.(map[string]interface{})
	if !ok {
		wm.sendToClient(client, &WSMessage{
			Type:    "error",
			Payload: map[string]string{"message": "无效的消息格式"},
		})
		return
	}

	content := payload["content"].(string)

	sendReq := &gen.ChatSendMessageReq{
		RoomId:     client.currentRoom,
		SenderId:   client.userID,
		SenderName: client.username,
		Content:    content,
		Type:       gen.ChatMsgType_TEXT,
	}

	resp, err := global.Rpc(client.currentRoom, string(types.Kind_Chat), "SendMessage", sendReq)
	if err != nil || resp == nil {
		log.Printf("发送消息请求失败: %v", err)
		wm.sendToClient(client, &WSMessage{
			Type:    "error",
			Payload: map[string]string{"message": "发送消息失败"},
		})
		return
	}

	if resp, ok := resp.(*gen.ChatSendMessageResp); ok {
		if resp.GetCode() != gen.ErrorCode_OK {
			wm.sendToClient(client, &WSMessage{
				Type:    "error",
				Payload: map[string]string{"message": resp.GetMessage()},
			})
			return
		} else {
			wm.sendToClient(client, &WSMessage{
				Type:    "new_message",
				Payload: resp.MessageData,
			})
		}
		return
	}
	wm.sendToClient(client, &WSMessage{
		Type:    "error",
		Payload: map[string]string{"message": "发送消息失败"},
	})
}

// handleGetRoomList 处理获取房间列表
func (wm *WebSocketManager) handleGetRoomList(client *WSClient, msg *WSMessage) {
	if client.userID == 0 {
		wm.sendToClient(client, &WSMessage{
			Type:    "error",
			Payload: map[string]string{"message": "请先登录"},
		})
		return
	}

	// 从全局房间注册表获取房间列表
	rooms := grains.GlobalRoomRegistry.GetAllRooms()

	// resp := &gen.ChatGetRoomListResp{
	// 	Code:    gen.ErrorCode_OK,
	// 	Message: "获取房间列表成功",
	// 	Rooms:   rooms,
	// 	Total:   int32(len(rooms)),
	// }

	wm.sendToClient(client, &WSMessage{
		Type:    "room_list",
		Payload: map[string]interface{}{"rooms": rooms},
	})
}

// handleGetRoomUsers 处理获取房间用户列表
func (wm *WebSocketManager) handleGetRoomUsers(client *WSClient, msg *WSMessage) {
	if client.userID == 0 || client.currentRoom == "" {
		wm.sendToClient(client, &WSMessage{
			Type:    "error",
			Payload: map[string]string{"message": "请先加入房间"},
		})
		return
	}

	getRoomUsersReq := &gen.ChatGetRoomUsersReq{
		RoomId: client.currentRoom,
	}

	resp, err := global.Rpc(client.currentRoom, string(types.Kind_Chat), "GetRoomUsers", getRoomUsersReq)
	if err != nil || resp == nil {
		log.Printf("获取房间用户列表请求失败: %v", err)
		wm.sendToClient(client, &WSMessage{
			Type:    "error",
			Payload: map[string]string{"message": "获取房间用户列表失败"},
		})
		return
	}
	if resp, ok := resp.(*gen.ChatGetRoomUsersResp); ok {
		wm.sendToClient(client, &WSMessage{
			Type:    "room_users",
			Payload: map[string]interface{}{"users": resp.GetUsers()},
		})
		return
	}
	wm.sendToClient(client, &WSMessage{
		Type:    "error",
		Payload: map[string]string{"message": "获取房间用户列表失败"},
	})
}

// unregisterClient 注销客户端
func (wm *WebSocketManager) unregisterClient(client *WSClient) {
	if client.userID == 0 {
		return
	}

	wm.clientsMu.Lock()
	delete(wm.clients, client.userID)
	wm.clientsMu.Unlock()

	log.Printf("用户 %d 已断开连接", client.userID)
}

// sendToClient 向客户端发送消息
func (wm *WebSocketManager) sendToClient(client *WSClient, msg *WSMessage) {
	data, err := json.Marshal(msg)
	if err != nil {
		log.Printf("JSON序列化错误: %v", err)
		return
	}
	log.Printf("发送消息到客户端: %s", string(data))
	select {
	case client.send <- data:
	default:
		close(client.send)
	}
}

// SendToUser 向指定用户发送消息（从Grain调用）
func (wm *WebSocketManager) SendToUser(userID int64, message interface{}) error {
	wm.clientsMu.RLock()
	client, exists := wm.clients[userID]
	wm.clientsMu.RUnlock()

	if !exists {
		return nil
	}

	var wsMsg *WSMessage
	switch m := message.(type) {
	case *gen.ChatMessage:
		wsMsg = &WSMessage{
			Type:    "new_message",
			Payload: m,
		}
	default:
		wsMsg = &WSMessage{
			Type:    "new_message",
			Payload: message,
		}
	}

	data, err := json.Marshal(wsMsg)
	if err != nil {
		return err
	}

	select {
	case client.send <- data:
		return nil
	default:
		return nil
	}
}

// GetOnlineUsers 获取在线用户列表
func (wm *WebSocketManager) GetOnlineUsers() []int64 {
	wm.clientsMu.RLock()
	defer wm.clientsMu.RUnlock()

	users := make([]int64, 0, len(wm.clients))
	for userID := range wm.clients {
		users = append(users, userID)
	}
	return users
}

// IsUserOnline 检查用户是否在线
func (wm *WebSocketManager) IsUserOnline(userID int64) bool {
	wm.clientsMu.RLock()
	defer wm.clientsMu.RUnlock()
	_, exists := wm.clients[userID]
	return exists
}
