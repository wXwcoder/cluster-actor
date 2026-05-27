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
	"github.com/cluster-actor/server/internal/cluster"
	"github.com/cluster-actor/server/internal/config"
	"github.com/cluster-actor/server/internal/grains"
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

	// 2. 创建集群服务器（基于 protoactor-go）
	server, err := cluster.NewServer(cfg)
	if err != nil {
		log.Fatalf("创建集群服务器失败: %v", err)
	}

	// 3. 初始化HTTP路由
	routerDeps := api.RouterDeps{
		Cfg:      cfg,
		NodeName: cfg.NodeName,
		Cluster:  server.GetCluster(),
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

	// 4. 启动服务器（包含集群启动和 Grain 注册）
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := server.Start(ctx); err != nil {
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
