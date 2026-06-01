package cluster

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sync"

	"github.com/asynkron/protoactor-go/actor"
	"github.com/asynkron/protoactor-go/cluster"
	"github.com/asynkron/protoactor-go/cluster/clusterproviders/consul"
	"github.com/asynkron/protoactor-go/cluster/identitylookup/disthash"
	"github.com/asynkron/protoactor-go/remote"
	"github.com/cluster-actor/server/internal/config"
	"github.com/cluster-actor/server/internal/global"
	"github.com/cluster-actor/server/internal/grains"
	"github.com/cluster-actor/server/internal/kvstore"
	"github.com/cluster-actor/server/pkg/types"
)

// Server 基于 protoactor-go 的集群服务器
type Server struct {
	cfg         *config.ClusterConfig
	actorSystem *actor.ActorSystem
	cluster     *cluster.Cluster
	kvStore     *kvstore.KVStore
	httpServer  *http.Server
	stopCh      chan struct{}
	isRunning   bool
	mu          sync.Mutex
}

// NewServer 创建集群服务器
func NewServer(cfg *config.ClusterConfig) (*Server, error) {
	// 创建 ActorSystem
	system := actor.NewActorSystem()

	// 创建 Consul Provider
	provider, err := consul.New()
	if err != nil {
		return nil, fmt.Errorf("创建Consul Provider失败: %w", err)
	}

	// 创建 DistHash 身份查找
	lookup := disthash.New()

	// 配置 Remote（gRPC 通信）
	remoteConfig := remote.Configure(cfg.Host, cfg.Port)

	// 创建 HelloGrain 的 Kind 配置
	helloKind := cluster.NewKind(string(types.Kind_Hello), actor.PropsFromProducer(func() actor.Actor {
		return grains.NewHelloGrain()
	}))
	userKind := cluster.NewKind(string(types.Kind_User), actor.PropsFromProducer(func() actor.Actor {
		return grains.NewUserGrain()
	}))
	chatKind := cluster.NewKind(string(types.Kind_Chat), actor.PropsFromProducer(func() actor.Actor {
		return grains.NewChatGrain()
	}))

	// 配置 Cluster
	clusterConfig := cluster.Configure(cfg.ClusterName, provider, lookup, remoteConfig, cluster.WithKinds(helloKind, chatKind, userKind))

	// 创建 Cluster 实例
	c := cluster.New(system, clusterConfig)

	// 创建分布式KV存储
	kv, err := kvstore.NewKVStore(cfg)
	if err != nil {
		return nil, fmt.Errorf("创建KVStore失败: %w", err)
	}

	srv := &Server{
		cfg:         cfg,
		actorSystem: system,
		cluster:     c,
		kvStore:     kv,
		stopCh:      make(chan struct{}),
	}

	// 初始化全局注册表
	global.G.Cfg = cfg
	global.G.Cluster = c
	return srv, nil
}

// Start 启动集群服务器
func (s *Server) Start(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.isRunning {
		return fmt.Errorf("服务器已在运行中")
	}

	// 启动集群成员
	s.cluster.StartMember()

	// 启动分布式KV存储的Watch监听
	if err := s.kvStore.Start(); err != nil {
		log.Printf("启动KVStore失败: %v", err)
	}

	s.isRunning = true
	log.Printf("集群服务器已启动: %s (地址: %s:%d, gRPC端口: %d)",
		s.cfg.NodeName, s.cfg.Host, s.cfg.HealthCheckPort, s.cfg.Port)

	return nil
}

// Stop 停止集群服务器
func (s *Server) Stop(ctx context.Context) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	if !s.isRunning {
		return nil
	}

	// 停止分布式KV存储的Watch监听
	if err := s.kvStore.Stop(); err != nil {
		log.Printf("停止KVStore失败: %v", err)
	}

	// 停止 HTTP 服务器
	if s.httpServer != nil {
		if err := s.httpServer.Shutdown(ctx); err != nil {
			log.Printf("关闭HTTP服务器失败: %v", err)
		}
	}

	// 关闭集群
	s.cluster.Shutdown(true)

	close(s.stopCh)
	s.isRunning = false
	log.Printf("集群服务器已停止: %s", s.cfg.NodeName)

	return nil
}

// GetCluster 获取 Cluster 实例
func (s *Server) GetCluster() *cluster.Cluster {
	return s.cluster
}

// GetActorSystem 获取 ActorSystem
func (s *Server) GetActorSystem() *actor.ActorSystem {
	return s.actorSystem
}

// GetRootContext 获取 RootContext
func (s *Server) GetRootContext() *actor.RootContext {
	return s.actorSystem.Root
}

// GetConfig 获取服务器配置
func (s *Server) GetConfig() *config.ClusterConfig {
	return s.cfg
}

// GetKVStore 获取分布式KV存储实例
func (s *Server) GetKVStore() *kvstore.KVStore {
	return s.kvStore
}

// IsRunning 检查服务器是否在运行
func (s *Server) IsRunning() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.isRunning
}

// StartHTTPServer 启动HTTP服务器
func (s *Server) StartHTTPServer(handler http.Handler, addr string) {
	s.httpServer = &http.Server{
		Addr:    addr,
		Handler: handler,
	}

	go func() {
		log.Printf("HTTP API服务器已启动: %s", addr)
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP API服务器错误: %v", err)
		}
	}()
}

// GetLocalGrainInstances 获取当前节点上激活的所有Grain实例
func (s *Server) GetLocalGrainInstances() []grains.GrainInstance {
	return grains.GlobalRegistry.GetInstances()
}

// GetNodeName 获取当前节点名称
func (s *Server) GetNodeName() string {
	return s.cfg.NodeName
}
