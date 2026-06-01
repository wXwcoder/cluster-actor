package config

// ClusterConfig 集群配置结构
type ClusterConfig struct {
	// ClusterName 集群名称
	ClusterName string `yaml:"cluster_name" json:"cluster_name"`

	// NodeName 节点名称（唯一标识）
	NodeName string `yaml:"node_name" json:"node_name"`

	// Host 监听地址
	Host string `yaml:"host" json:"host"`

	// Port 监听端口
	Port int `yaml:"port" json:"port"`

	// Consul Consul配置
	Consul ConsulConfig `yaml:"consul" json:"consul"`

	// Actor Actor系统配置
	Actor ActorConfig `yaml:"actor" json:"actor"`

	// LogLevel 日志级别 (debug, info, warn, error)
	LogLevel string `yaml:"log_level" json:"log_level"`

	// HealthCheckPort 健康检查端口
	HealthCheckPort int `yaml:"health_check_port" json:"health_check_port"`

	// Tracing 链路追踪配置
	Tracing TracingConfig `yaml:"tracing" json:"tracing"`
}

// GetClusterName 获取集群名称
func (c *ClusterConfig) GetClusterName() string {
	return c.ClusterName
}

// GetNodeName 获取节点名称
func (c *ClusterConfig) GetNodeName() string {
	return c.NodeName
}

// GetHost 获取主机地址
func (c *ClusterConfig) GetHost() string {
	return c.Host
}

// GetPort 获取端口
func (c *ClusterConfig) GetPort() int {
	return c.Port
}

// ConsulConfig Consul服务发现配置
type ConsulConfig struct {
	// Address Consul地址，例如 "127.0.0.1:8500"
	Address string `yaml:"address" json:"address"`

	// Scheme 协议 (http/https)
	Scheme string `yaml:"scheme" json:"scheme"`

	// Token Consul ACL Token
	Token string `yaml:"token" json:"token"`

	// Datacenter 数据中心名称
	Datacenter string `yaml:"datacenter" json:"datacenter"`

	// CachePrefix 缓存前缀
	CachePrefix string `yaml:"cache_prefix" json:"cache_prefix"`

	// TTLHeartbeatInterval TTL心跳间隔（秒）
	TTLHeartbeatInterval int `yaml:"ttl_heartbeat_interval" json:"ttl_heartbeat_interval"`

	// DeregisterCriticalServiceAfter 服务异常后自动注销时间（秒）
	DeregisterCriticalServiceAfter int `yaml:"deregister_critical_service_after" json:"deregister_critical_service_after"`
}

// ActorConfig Actor系统配置
type ActorConfig struct {
	// MaxGrainCount 单个节点最大Grain数量
	MaxGrainCount int `yaml:"max_grain_count" json:"max_grain_count"`

	// ActivationTimeout Grain激活超时时间（毫秒）
	ActivationTimeout int `yaml:"activation_timeout" json:"activation_timeout"`

	// IdleTimeout Grain空闲超时时间（分钟）
	IdleTimeout int `yaml:"idle_timeout" json:"idle_timeout"`

	// RemoteCallTimeout 远程调用超时时间（毫秒）
	RemoteCallTimeout int `yaml:"remote_call_timeout" json:"remote_call_timeout"`
}

// TracingConfig 链路追踪配置
type TracingConfig struct {
	// Enabled 是否启用链路追踪
	Enabled bool `yaml:"enabled" json:"enabled"`

	// Type 追踪类型 (zipkin, jaeger)
	Type string `yaml:"type" json:"type"`

	// Endpoint 追踪服务器地址
	Endpoint string `yaml:"endpoint" json:"endpoint"`

	// SampleRatio 采样率 (0.0-1.0)
	SampleRatio float64 `yaml:"sample_ratio" json:"sample_ratio"`
}
