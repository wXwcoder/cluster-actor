// Package api 提供HTTP API路由和处理
package api

import (
	"context"
	"net/http"

	"github.com/asynkron/protoactor-go/cluster"
	"github.com/cluster-actor/server/internal/config"
	"github.com/cluster-actor/server/internal/grains"
	"github.com/cluster-actor/server/internal/kvstore"
	"github.com/cluster-actor/server/pkg/types"
	"github.com/gin-gonic/gin"
)

// Router HTTP路由管理器
type Router struct {
	engine            *gin.Engine
	cfg               *config.ClusterConfig
	nodeName          string
	cluster           *cluster.Cluster
	kvStore           *kvstore.KVStore
	getMembers        func() ([]*types.MemberInfo, error)
	memberCount       func() int
	isRunning         func() bool
	getLocalInstances func() []grains.GrainInstance
	getKindNames      func() []string
	wsManager         *WebSocketManager
}

// RouterDeps 路由器依赖项
type RouterDeps struct {
	Cfg               *config.ClusterConfig
	NodeName          string
	Cluster           *cluster.Cluster
	KVStore           *kvstore.KVStore
	GetMembers        func() ([]*types.MemberInfo, error)
	MemberCount       func() int
	IsRunning         func() bool
	GetLocalInstances func() []grains.GrainInstance
	GetKindNames      func() []string
}

// TopologyEvent 拓扑变化事件
type TopologyEvent struct {
	Type    string
	Members []*types.MemberInfo
}

// NewRouter 创建路由管理器
func NewRouter(deps RouterDeps) *Router {
	gin.SetMode(gin.ReleaseMode)
	wsManager := NewWebSocketManager(deps.Cluster)
	r := &Router{
		engine:            gin.New(),
		cfg:               deps.Cfg,
		nodeName:          deps.NodeName,
		cluster:           deps.Cluster,
		kvStore:           deps.KVStore,
		getMembers:        deps.GetMembers,
		memberCount:       deps.MemberCount,
		isRunning:         deps.IsRunning,
		getLocalInstances: deps.GetLocalInstances,
		getKindNames:      deps.GetKindNames,
		wsManager:         wsManager,
	}

	r.setupRoutes()
	return r
}

// setupRoutes 设置路由
func (r *Router) setupRoutes() {
	// 使用中间件
	r.engine.Use(gin.Recovery())
	r.engine.Use(gin.Logger())

	// 健康检查
	r.engine.GET("/health", r.healthCheck)

	// WebSocket路由
	r.engine.GET("/ws", func(c *gin.Context) {
		r.wsManager.HandleWebSocket(c.Writer, c.Request)
	})

	// 静态文件服务
	r.engine.Static("/static", "./web/static")
	r.engine.StaticFile("/", "./web/index.html")
	r.engine.StaticFile("/chat", "./web/chat.html")
	r.engine.StaticFile("/dashboard", "./web/dashboard.html")

	// 集群相关API
	clusterGroup := r.engine.Group("/api/cluster")
	{
		clusterGroup.GET("/info", r.getClusterInfo)
		clusterGroup.GET("/members", r.getClusterMembers)
		clusterGroup.GET("/status", r.getClusterStatus)
	}

	// Actor相关API
	actorGroup := r.engine.Group("/api/actor")
	{
		actorGroup.GET("/instances", r.getActorInstances)
		actorGroup.GET("/local-instances", r.getLocalActorInstances)
		actorGroup.GET("/kinds", r.getGrainKinds)
		actorGroup.GET("/call/:kind/:identity", r.GrainCall)
	}

	// kvstore相关API
	kvstoreGroup := r.engine.Group("/api/kvstore")
	{
		kvstoreGroup.GET("/status", r.getKVStoreStatus)
		kvstoreGroup.GET("/key/:key", r.getKVStoreKey)
	}

}

// GetEngine 获取Gin引擎
func (r *Router) GetEngine() *gin.Engine {
	return r.engine
}

// StartHTTPServer 启动HTTP服务器
func (r *Router) StartHTTPServer(ctx context.Context, addr string) (*http.Server, error) {
	srv := &http.Server{
		Addr:    addr,
		Handler: r.engine,
	}

	go func() {
		<-ctx.Done()
		srv.Shutdown(context.Background())
	}()

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			// 记录错误但不崩溃
		}
	}()

	return srv, nil
}
