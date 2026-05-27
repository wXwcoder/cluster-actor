package config_test

import (
	"os"
	"testing"

	"github.com/cluster-actor/server/internal/config"
)

func TestLoadDefaultConfig(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "config-*.yaml")
	if err != nil {
		t.Fatalf("创建临时文件失败: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	configContent := `
cluster_name: "test-cluster"
node_name: "test-node"
host: "127.0.0.1"
port: 9090
consul:
  address: "127.0.0.1:8500"
  scheme: "http"
actor:
  max_grain_count: 5000
`
	if _, err := tmpFile.WriteString(configContent); err != nil {
		t.Fatalf("写入配置内容失败: %v", err)
	}
	tmpFile.Close()

	cfg, err := config.Load(tmpFile.Name())
	if err != nil {
		t.Fatalf("加载配置失败: %v", err)
	}

	if cfg.ClusterName != "test-cluster" {
		t.Errorf("期望 ClusterName=test-cluster, 实际=%s", cfg.ClusterName)
	}
	if cfg.NodeName != "test-node" {
		t.Errorf("期望 NodeName=test-node, 实际=%s", cfg.NodeName)
	}
	if cfg.Port != 9090 {
		t.Errorf("期望 Port=9090, 实际=%d", cfg.Port)
	}
	if cfg.Actor.MaxGrainCount != 5000 {
		t.Errorf("期望 MaxGrainCount=5000, 实际=%d", cfg.Actor.MaxGrainCount)
	}
}

func TestMinimalConfig(t *testing.T) {
	tmpFile, err := os.CreateTemp("", "config-*.yaml")
	if err != nil {
		t.Fatalf("创建临时文件失败: %v", err)
	}
	defer os.Remove(tmpFile.Name())

	configContent := `
cluster_name: "minimal-cluster"
`
	if _, err := tmpFile.WriteString(configContent); err != nil {
		t.Fatalf("写入配置内容失败: %v", err)
	}
	tmpFile.Close()

	cfg, err := config.Load(tmpFile.Name())
	if err != nil {
		t.Fatalf("加载配置失败: %v", err)
	}

	if cfg.ClusterName != "minimal-cluster" {
		t.Errorf("期望 ClusterName=minimal-cluster, 实际=%s", cfg.ClusterName)
	}
	if cfg.Port != 8080 {
		t.Errorf("期望默认 Port=8080, 实际=%d", cfg.Port)
	}
}
