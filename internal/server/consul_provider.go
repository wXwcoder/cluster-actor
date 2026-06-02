package server

import (
	"context"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/cluster-actor/server/internal/config"
	"github.com/cluster-actor/server/pkg/types"
	"github.com/hashicorp/consul/api"
)

// ClusterProvider 集群提供者接口
type ClusterProvider interface {
	// Start 启动集群提供者
	Start(ctx context.Context) error
	// Stop 停止集群提供者
	Stop(ctx context.Context) error
	// RegisterService 注册服务到服务发现
	RegisterService(nodeName string, address string, port int, kindNames []string) error
	// DeregisterService 从服务发现注销服务
	DeregisterService(nodeName string) error
	// GetMembers 获取集群成员列表
	GetMembers() ([]*types.MemberInfo, error)
	// SubscribeTopology 订阅拓扑变化
	SubscribeTopology() <-chan TopologyEvent
	// SendHeartbeat 发送心跳
	SendHeartbeat(nodeName string) error
	// MemberCount 获取集群成员数量
	MemberCount() int
}

// TopologyEvent 拓扑变化事件
type TopologyEvent struct {
	Type    TopologyEventType
	Members []*types.MemberInfo
}

// TopologyEventType 拓扑事件类型
type TopologyEventType string

const (
	TopologyChanged   TopologyEventType = "changed"
	TopologyBlocked   TopologyEventType = "blocked"
	TopologyUnblocked TopologyEventType = "unblocked"
)

// ConsulProvider Consul服务发现提供者
type ConsulProvider struct {
	cfg        *config.ClusterConfig
	client     *api.Client
	mu         sync.RWMutex
	members    map[string]*types.MemberInfo
	topologyCh chan TopologyEvent
	stopCh     chan struct{}
	isRunning  bool
}

// NewConsulProvider 创建Consul提供者
func NewConsulProvider(cfg *config.ClusterConfig) (*ConsulProvider, error) {
	consulCfg := api.DefaultConfig()
	consulCfg.Address = cfg.Consul.Address
	consulCfg.Scheme = cfg.Consul.Scheme
	if cfg.Consul.Token != "" {
		consulCfg.Token = cfg.Consul.Token
	}

	client, err := api.NewClient(consulCfg)
	if err != nil {
		return nil, fmt.Errorf("创建Consul客户端失败: %w", err)
	}

	return &ConsulProvider{
		cfg:        cfg,
		client:     client,
		members:    make(map[string]*types.MemberInfo),
		topologyCh: make(chan TopologyEvent, 10),
		stopCh:     make(chan struct{}),
	}, nil
}

// Start 启动Consul提供者
func (p *ConsulProvider) Start(ctx context.Context) error {
	if p.isRunning {
		return fmt.Errorf("Consul提供者已在运行中")
	}

	// 检查Consul连接
	_, err := p.client.Status().Leader()
	if err != nil {
		return fmt.Errorf("无法连接到Consul: %w", err)
	}

	p.isRunning = true
	log.Printf("Consul提供者已启动, 地址: %s", p.cfg.Consul.Address)

	// 启动拓扑监控
	go p.watchTopology(ctx)

	return nil
}

// Stop 停止Consul提供者
func (p *ConsulProvider) Stop(ctx context.Context) error {
	if !p.isRunning {
		return nil
	}

	close(p.stopCh)
	p.isRunning = false
	close(p.topologyCh)
	log.Printf("Consul提供者已停止")
	return nil
}

// RegisterService 注册服务到Consul
func (p *ConsulProvider) RegisterService(nodeName string, address string, port int, kindNames []string) error {
	serviceID := fmt.Sprintf("%s-%s", p.cfg.ClusterName, nodeName)

	registration := &api.AgentServiceRegistration{
		ID:      serviceID,
		Name:    p.cfg.ClusterName,
		Address: address,
		Port:    port,
		Tags:    kindNames,
		Meta: map[string]string{
			"node_name": nodeName,
			"cluster":   p.cfg.ClusterName,
			"kinds":     fmt.Sprintf("%v", kindNames),
		},
		Check: &api.AgentServiceCheck{
			TTL:                            fmt.Sprintf("%ds", p.cfg.Consul.TTLHeartbeatInterval*3),
			DeregisterCriticalServiceAfter: fmt.Sprintf("%ds", p.cfg.Consul.DeregisterCriticalServiceAfter),
		},
	}

	err := p.client.Agent().ServiceRegister(registration)
	if err != nil {
		return fmt.Errorf("注册服务到Consul失败: %w", err)
	}

	log.Printf("服务已注册到Consul: %s (地址: %s:%d)", nodeName, address, port)
	return nil
}

// DeregisterService 从Consul注销服务
func (p *ConsulProvider) DeregisterService(nodeName string) error {
	serviceID := fmt.Sprintf("%s-%s", p.cfg.ClusterName, nodeName)

	err := p.client.Agent().ServiceDeregister(serviceID)
	if err != nil {
		return fmt.Errorf("从Consul注销服务失败: %w", err)
	}

	log.Printf("服务已从Consul注销: %s", nodeName)
	return nil
}

// GetMembers 获取集群成员列表
func (p *ConsulProvider) GetMembers() ([]*types.MemberInfo, error) {
	services, _, err := p.client.Health().Service(p.cfg.ClusterName, "", false, nil)
	if err != nil {
		return nil, fmt.Errorf("查询Consul服务失败: %w", err)
	}

	var members []*types.MemberInfo
	for _, svc := range services {
		member := &types.MemberInfo{
			NodeName: svc.Service.Meta["node_name"],
			Address:  svc.Service.Address,
			Port:     svc.Service.Port,
			Alive:    svc.Checks.AggregatedStatus() == "passing",
			LastSeen: time.Now(),
		}
		members = append(members, member)
	}

	return members, nil
}

// SubscribeTopology 订阅拓扑变化
func (p *ConsulProvider) SubscribeTopology() <-chan TopologyEvent {
	return p.topologyCh
}

// SendHeartbeat 发送心跳到Consul
func (p *ConsulProvider) SendHeartbeat(nodeName string) error {
	serviceID := fmt.Sprintf("%s-%s", p.cfg.ClusterName, nodeName)
	checkID := fmt.Sprintf("service:%s", serviceID)

	err := p.client.Agent().UpdateTTL(checkID, "heartbeat ok", "passing")
	if err != nil {
		return fmt.Errorf("发送心跳失败: %w", err)
	}

	return nil
}

// watchTopology 监控集群拓扑变化
func (p *ConsulProvider) watchTopology(ctx context.Context) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			log.Printf("拓扑监控已停止")
			return
		case <-p.stopCh:
			log.Printf("拓扑监控已停止")
			return
		case <-ticker.C:
			p.updateTopology(ctx)
		}
	}
}

// updateTopology 更新拓扑信息
func (p *ConsulProvider) updateTopology(ctx context.Context) {
	members, err := p.GetMembers()
	if err != nil {
		log.Printf("获取集群成员失败: %v", err)
		return
	}

	// 检查成员变化
	p.mu.Lock()
	changed := false

	// 检查新加入或离开的成员
	currentNodes := make(map[string]bool)
	for _, member := range members {
		currentNodes[member.NodeName] = true
		if _, exists := p.members[member.NodeName]; !exists {
			changed = true
			log.Printf("新节点加入集群: %s", member.NodeName)
		} else {
			// 更新状态
			old := p.members[member.NodeName]
			if old.Alive != member.Alive {
				changed = true
			}
		}
	}

	// 检查离开的节点
	for name := range p.members {
		if !currentNodes[name] {
			changed = true
			log.Printf("节点离开集群: %s", name)
		}
	}

	// 更新成员列表
	p.members = make(map[string]*types.MemberInfo)
	for _, member := range members {
		p.members[member.NodeName] = member
	}
	p.mu.Unlock()

	// 如果有变化，发送拓扑事件
	if changed {
		select {
		case p.topologyCh <- TopologyEvent{
			Type:    TopologyChanged,
			Members: members,
		}:
		default:
			log.Printf("拓扑事件通道已满，跳过")
		}
	}
}

// GetMemberByName 根据名称获取成员
func (p *ConsulProvider) GetMemberByName(nodeName string) (*types.MemberInfo, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()

	member, exists := p.members[nodeName]
	if !exists {
		return nil, fmt.Errorf("节点不存在: %s", nodeName)
	}

	return member, nil
}

// MemberCount 获取集群成员数量
func (p *ConsulProvider) MemberCount() int {
	p.mu.RLock()
	defer p.mu.RUnlock()
	return len(p.members)
}
