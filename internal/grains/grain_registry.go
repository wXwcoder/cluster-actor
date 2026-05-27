// Package grains 提供基于 protoactor-go 的虚拟 Actor 定义和实现
package grains

import (
	"log"
	"sync"
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
