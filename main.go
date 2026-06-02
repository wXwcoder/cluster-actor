// Package main ClusterActor 集群服务器
// 基于 protoactor-go 实现 Virtual Actor Model
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/cluster-actor/server/internal/api"
	"github.com/cluster-actor/server/internal/config"
	"github.com/cluster-actor/server/internal/global"
	"github.com/cluster-actor/server/internal/grains"
	"github.com/cluster-actor/server/internal/server"
	"github.com/cluster-actor/server/internal/telemetry"
	"github.com/cluster-actor/server/pkg/types"
)

// Version 当前版本号
var Version = "0.0.1"

// main 是程序入口点
// 支持通过 -config 参数指定配置文件路径，默认为 configs/config.yaml
// 支持通过 -version 参数打印版本号并退出
func main() {
	// 解析命令行参数
	configPath := flag.String("config", "configs/config.yaml", "配置文件路径")
	showVersion := flag.Bool("version", false, "打印版本号")
	flag.Parse()

	// 处理 version 参数
	if *showVersion {
		fmt.Printf("Version: %s\n", Version)
		return
	}

	fmt.Printf("Version: %s\n", Version)
	log.SetFlags(log.LstdFlags | log.Lshortfile)

	// 1. 加载配置
	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Fatalf("加载配置失败: %v", err)
	}

	// 2. 初始化 Zipkin 追踪
	ctx := context.Background()
	if cfg.Tracing.Enabled {
		if cfg.Tracing.Type == "zipkin" {
			zipkinProvider, err := telemetry.InitZipkin(ctx, cfg.ClusterName, cfg.Tracing.Endpoint, cfg.Tracing.SampleRatio)
			if err != nil {
				log.Fatalf("初始化 Zipkin 追踪失败：%v", err)
			}
			defer func() {
				shutdownCtx, cancel := context.WithTimeout(context.Background(), 5000)
				defer cancel()
				if err := zipkinProvider.Shutdown(shutdownCtx); err != nil {
					log.Printf("关闭 Zipkin 追踪失败：%v", err)
				}
			}()
			log.Printf("Zipkin 追踪已启用：端点=%s, 采样率=%.2f", cfg.Tracing.Endpoint, cfg.Tracing.SampleRatio)

			// 创建测试 span 验证 Zipkin 连接
			telemetry.TestSpan(ctx, cfg.ClusterName)
			log.Printf("已发送测试 span 到 Zipkin UI 中查看kin UI 中查看")
		} else if cfg.Tracing.Type == "jaeger" {
			jaegerProvider := telemetry.InitJaeger()
			defer jaegerProvider.Close()
			log.Printf("Jaeger 追踪已启用：端点=%s, 采样率=%.2f", cfg.Tracing.Endpoint, cfg.Tracing.SampleRatio)
		}

	} else {
		log.Printf("链路追踪已禁用")
	}

	// 2. 创建集群服务器（基于 protoactor-go）
	server, err := server.NewServer(cfg)
	if err != nil {
		log.Fatalf("创建集群服务器失败: %v", err)
	}

	// 初始化全局注册表
	global.G.Cfg = cfg
	global.G.Cluster = server.GetCluster()
	global.G.KVStore = server.GetKVStore()

	// 3. 初始化HTTP路由
	routerDeps := api.RouterDeps{
		Cfg:      cfg,
		NodeName: cfg.NodeName,
		Cluster:  server.GetCluster(),
		KVStore:  server.GetKVStore(),
		GetMembers: func() ([]*types.MemberInfo, error) {
			// 使用 protoactor cluster 的 MemberList 获取集群成员
			memberSet := server.GetCluster().MemberList.Members()
			result := make([]*types.MemberInfo, 0, memberSet.Len())
			for _, m := range memberSet.Members() {
				result = append(result, &types.MemberInfo{
					NodeName: m.Id,
					Address:  fmt.Sprintf("%s:%d", cfg.Host, m.Port),
					Alive:    true,
				})
			}
			return result, nil
		},
		MemberCount: func() int {
			return server.GetCluster().MemberList.Members().Len()
		},
		IsRunning: server.IsRunning,
		GetLocalInstances: func() []grains.GrainInstance {
			return server.GetLocalGrainInstances()
		},
		GetKindNames: func() []string {
			return grains.GlobalRegistry.GetKindNames()
		},
	}
	router := api.NewRouter(routerDeps)

	// 3. 启动服务器（包含集群启动和 Grain 注册）
	serverCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := server.Start(serverCtx); err != nil {
		log.Fatalf("启动集群服务器失败: %v", err)
	}

	// 5. 启动HTTP API服务器
	apiAddr := fmt.Sprintf("%s:%d", cfg.Host, cfg.HealthCheckPort)
	server.StartHTTPServer(router.GetEngine(), apiAddr)

	// 6. 等待退出信号
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	log.Printf("集群服务器已就绪, 等待信号...")
	sig := <-sigCh
	log.Printf("收到信号: %v, 正在关闭...", sig)

	// 7. 优雅关闭
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5000)
	defer shutdownCancel()

	if err := server.Stop(shutdownCtx); err != nil {
		log.Printf("关闭服务器时出错: %v", err)
	}

	log.Printf("服务器已安全关闭")
}
