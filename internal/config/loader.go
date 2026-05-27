package config

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// Load 加载配置文件，支持YAML和JSON格式
func Load(configPath string) (*ClusterConfig, error) {
	if configPath == "" {
		configPath = "configs/config.yaml"
	}

	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("读取配置文件失败: %w", err)
	}

	var cfg ClusterConfig
	
	// 根据文件扩展名选择解析方式
	if strings.HasSuffix(configPath, ".json") {
		err = json.Unmarshal(data, &cfg)
	} else {
		err = yaml.Unmarshal(data, &cfg)
	}
	
	if err != nil {
		return nil, fmt.Errorf("解析配置文件失败: %w", err)
	}

	// 应用环境变量覆盖
	applyEnvOverride(&cfg)

	// 设置默认值
	setDefaults(&cfg)

	// 验证配置
	if err := validate(&cfg); err != nil {
		return nil, fmt.Errorf("配置验证失败: %w", err)
	}

	log.Printf("配置加载成功: 集群=%s, 节点=%s", cfg.ClusterName, cfg.NodeName)
	return &cfg, nil
}

// applyEnvOverride 应用环境变量覆盖配置
func applyEnvOverride(cfg *ClusterConfig) {
	if v := os.Getenv("CLUSTER_NAME"); v != "" {
		cfg.ClusterName = v
	}
	if v := os.Getenv("NODE_NAME"); v != "" {
		cfg.NodeName = v
	}
	if v := os.Getenv("HOST"); v != "" {
		cfg.Host = v
	}
	if v := os.Getenv("PORT"); v != "" {
		fmt.Sscanf(v, "%d", &cfg.Port)
	}
	if v := os.Getenv("CONSUL_ADDRESS"); v != "" {
		cfg.Consul.Address = v
	}
	if v := os.Getenv("LOG_LEVEL"); v != "" {
		cfg.LogLevel = v
	}
}

// setDefaults 设置配置默认值
func setDefaults(cfg *ClusterConfig) {
	if cfg.ClusterName == "" {
		cfg.ClusterName = "default-cluster"
	}
	if cfg.NodeName == "" {
		cfg.NodeName = fmt.Sprintf("node-%d", os.Getpid())
	}
	if cfg.Host == "" {
		cfg.Host = "127.0.0.1"
	}
	if cfg.Port == 0 {
		cfg.Port = 8080
	}
	if cfg.Consul.Address == "" {
		cfg.Consul.Address = "127.0.0.1:8500"
	}
	if cfg.Consul.Scheme == "" {
		cfg.Consul.Scheme = "http"
	}
	if cfg.Consul.TTLHeartbeatInterval == 0 {
		cfg.Consul.TTLHeartbeatInterval = 10
	}
	if cfg.Consul.DeregisterCriticalServiceAfter == 0 {
		cfg.Consul.DeregisterCriticalServiceAfter = 30
	}
	if cfg.Actor.MaxGrainCount == 0 {
		cfg.Actor.MaxGrainCount = 10000
	}
	if cfg.Actor.ActivationTimeout == 0 {
		cfg.Actor.ActivationTimeout = 3000
	}
	if cfg.Actor.IdleTimeout == 0 {
		cfg.Actor.IdleTimeout = 30
	}
	if cfg.Actor.RemoteCallTimeout == 0 {
		cfg.Actor.RemoteCallTimeout = 5000
	}
	if cfg.LogLevel == "" {
		cfg.LogLevel = "info"
	}
	if cfg.HealthCheckPort == 0 {
		cfg.HealthCheckPort = cfg.Port + 100
	}
}

// validate 验证配置合法性
func validate(cfg *ClusterConfig) error {
	if cfg.ClusterName == "" {
		return fmt.Errorf("集群名称不能为空")
	}
	if cfg.Port <= 0 || cfg.Port > 65535 {
		return fmt.Errorf("端口号必须在1-65535范围内")
	}
	if cfg.Consul.Address == "" {
		return fmt.Errorf("Consul地址不能为空")
	}
	validLogLevels := map[string]bool{
		"debug": true,
		"info":  true,
		"warn":  true,
		"error": true,
	}
	if !validLogLevels[cfg.LogLevel] {
		return fmt.Errorf("无效的日志级别: %s", cfg.LogLevel)
	}
	return nil
}
