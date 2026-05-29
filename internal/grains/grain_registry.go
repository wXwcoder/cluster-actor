// Package grains 提供基于 protoactor-go 的虚拟 Actor 定义和实现
package grains

import (
	"log"
	"sync"

	"github.com/cluster-actor/server/gen"
)

// GrainInstance 本地Grain实例信息
type GrainInstance struct {
	KindName string
	Identity string
}

// GrainRegistry 本地Grain实例注册表，用于追踪当前节点上激活的所有Grain实例
type GrainRegistry struct {
	mu        sync.RWMutex
	instances map[string]GrainInstance // key = kindName + ":" + identity
}

// NewGrainRegistry 创建新的Grain注册表
func NewGrainRegistry() *GrainRegistry {
	return &GrainRegistry{
		instances: make(map[string]GrainInstance),
	}
}

// Register 注册一个Grain实例
func (r *GrainRegistry) Register(kindName, identity string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := kindName + ":" + identity
	r.instances[key] = GrainInstance{
		KindName: kindName,
		Identity: identity,
	}
	log.Printf("Grain Register: %s, %s", kindName, identity)
}

// Unregister 注销一个Grain实例
func (r *GrainRegistry) Unregister(kindName, identity string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	key := kindName + ":" + identity
	delete(r.instances, key)
	log.Printf("Grain Unregister: %s, %s", kindName, identity)
}

// GetInstances 获取所有已注册的Grain实例
func (r *GrainRegistry) GetInstances() []GrainInstance {
	r.mu.RLock()
	defer r.mu.RUnlock()

	instances := make([]GrainInstance, 0, len(r.instances))
	for _, instance := range r.instances {
		instances = append(instances, instance)
	}
	return instances
}

// Count 获取已注册的Grain实例数量
func (r *GrainRegistry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return len(r.instances)
}

// GetKindNames 获取所有已注册的Grain Kind名称列表
func (r *GrainRegistry) GetKindNames() []string {
	r.mu.RLock()
	defer r.mu.RUnlock()

	kinds := make(map[string]bool)
	for _, instance := range r.instances {
		kinds[instance.KindName] = true
	}

	kindNames := make([]string, 0, len(kinds))
	for kindName := range kinds {
		kindNames = append(kindNames, kindName)
	}
	return kindNames
}

// GlobalRegistry 全局Grain注册表实例
var GlobalRegistry = NewGrainRegistry()

// RoomRegistry 房间注册表，用于追踪所有已创建的聊天室
type RoomRegistry struct {
	mu    sync.RWMutex
	rooms map[string]*gen.ChatRoomInfo // key = roomId
}

// NewRoomRegistry 创建新的房间注册表
func NewRoomRegistry() *RoomRegistry {
	return &RoomRegistry{
		rooms: make(map[string]*gen.ChatRoomInfo),
	}
}

// RegisterRoom 注册一个房间
func (r *RoomRegistry) RegisterRoom(room *gen.ChatRoomInfo) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.rooms[room.GetRoomId()] = room
	log.Printf("房间注册: %+v", room)
}

// UnregisterRoom 注销一个房间
func (r *RoomRegistry) UnregisterRoom(roomId string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	delete(r.rooms, roomId)
	log.Printf("房间注销: %s", roomId)
}

// GetRoom 获取指定房间信息
func (r *RoomRegistry) GetRoom(roomId string) (*gen.ChatRoomInfo, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	room, exists := r.rooms[roomId]
	return room, exists
}

// UpdateRoomMembers 更新房间成员数
func (r *RoomRegistry) UpdateRoomMembers(roomId string, currentMembers int32) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if room, exists := r.rooms[roomId]; exists {
		room.CurrentMembers = currentMembers
	}
}

// GetAllRooms 获取所有房间列表
// 修复GetAllRooms方法返回类型与RoomRegistry结构体不一致的问题
func (r *RoomRegistry) GetAllRooms() []*gen.ChatRoomInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()

	rooms := make([]*gen.ChatRoomInfo, 0, len(r.rooms))
	for _, room := range r.rooms {
		rooms = append(rooms, room)
	}
	return rooms
}

// GetRoomCount 获取房间数量
func (r *RoomRegistry) GetRoomCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()

	return len(r.rooms)
}

// GlobalRoomRegistry 全局房间注册表实例
var GlobalRoomRegistry = NewRoomRegistry()
