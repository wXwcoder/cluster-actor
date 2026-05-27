# Cluster Actor

基于 ProtoActor Go 的分布式集群 Actor 框架，提供 Virtual Actor (Grain) 模型、服务发现、负载均衡和故障恢复能力。

## 技术栈

| 组件 | 说明 |
|------|------|
| **ProtoActor Go** | Virtual Actor Model 框架，提供 Grain 抽象和集群管理 |
| **Consul** | 服务发现和集群成员管理 |
| **gRPC** | 节点间高性能通信 |
| **DistHash** | 分布式一致性哈希，实现 Grain 负载均衡 |

## 架构设计

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                           Cluster Topology                                  │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│   ┌─────────────────────────┐          ┌─────────────────────────┐          │
│   │        Node 1           │          │        Node 2           │          │
│   │  ┌────────────────────┐ │          │  ┌────────────────────┐ │          │
│   │  │   Remote (gRPC)    │ │          │  │   Remote (gRPC)    │ │          │
│   │  └─────────┬──────────┘ │          │  └─────────┬──────────┘ │          │
│   │            │            │          │            │            │          │
│   │  ┌─────────▼──────────┐ │          │  ┌─────────▼──────────┐ │          │
│   │  │   Cluster Manager  │ │          │  │   Cluster Manager  │ │          │ 
│   │  │    (Grains/Actor)  │ │          │  │    (Grains/Actor)  │ │          │
│   │  │  ┌──────────────┐  │ │          │  │  ┌──────────────┐  │ │          │
│   │  │  │   Actor 1    │  │ │          │  │  │   Actor 3    │  │ │          │
│   │  │  │   Actor 2    │  │ │          │  │  │   Actor 4    │  │ │          │
│   │  │  └──────────────┘  │ │          │  │  └──────────────┘  │ │          │ 
│   │  └─────────┬──────────┘ │          │  └─────────┬──────────┘ │          │
│   │            │            │          │            │            │          │
│   │  ┌─────────▼──────────┐ │          │  ┌─────────▼──────────┐ │          │
│   │  │  Identity Lookup   │ │          │  │  Identity Lookup   │ │          │
│   │  │    (DistHash)      │ │          │  │    (DistHash)      │ │          │ 
│   │  └────────────────────┘ │          │  └────────────────────┘ │          │
│   └────────────┬────────────┘          └────────────┬────────────┘          │
│                │                                    │                       │
│                └──────────────────┬─────────────────┘                       │
│                                   │                                         │
│                    ┌──────────────▼──────────────┐                          │
│                    │    Cluster Provider         │                          │
│                    │        (Consul)             │                          │
│                    │  - Service Discovery        │                          │
│                    │  - Member Management        │                          │
│                    └─────────────────────────────┘                          │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 核心特性

- **Virtual Actor 模型**：通过逻辑 ID 访问 Actor，框架自动路由到正确节点
- **服务发现**：基于 Consul 的自动服务注册和发现
- **负载均衡**：分布式一致性哈希算法，相同 identity 始终路由到同一节点
- **故障恢复**：节点失效后 Grain 自动在其他节点重新激活
- **HTTP API**：提供集群状态查询和 Grain 调用接口
- **结构化日志**：基于 slog 的日志系统

## 项目结构

```
cluster-actor/
├── main.go                    # 主程序入口
├── gen/                       # 自动生成的 Protobuf 代码
├── internal/
│   ├── config/                # 配置管理
│   ├── grains/                # Grain 实现
│   ├── api/                   # HTTP API
│   └── cluster/               # 集群管理
├── pkg/
│   └── types/                 # 公共类型定义
├── proto/                     # Protobuf 定义
├── configs/                   # 配置文件
├── tests/                     # 测试目录
├── go.mod                     # Go 模块依赖
├── go.sum                     # 依赖校验和
├── LICENSE                    # 许可证
├── .gitignore                 # Git 忽略配置
├── plan.md                    # 项目计划
├── proto.bat                  # Protobuf 编译脚本
└── start.bat                  # 多节点启动脚本
```

## 快速开始

### 环境要求

- Go 1.21+
- Consul
- protoc 和 protoc-gen-go

### 安装依赖

```bash
cd e:\xcode\cluster-actor
go mod tidy
```

### 启动 Consul

```bash
consul agent -dev
```

### 运行节点

```bash
# 节点1
go run main.go -config configs/config1.yaml

# 节点2（不同端口）
go run main.go -config configs/config2.yaml
```

或使用多节点启动脚本：

```bash
start.bat
```

## 配置说明

配置文件示例（`configs/config1.yaml`）：

```yaml
cluster:
  name: "my-cluster"
  host: "127.0.0.1"
  port: 8080
consul:
  address: "127.0.0.1:8500"
```

多节点部署时，复制配置文件并修改端口号：
- `configs/config1.yaml` - 节点1配置（端口 8080）
- `configs/config2.yaml` - 节点2配置（端口 8081）

## API 接口

| 端点 | 方法 | 描述 |
|------|------|------|
| `/health` | GET | 健康检查 |
| `/cluster/members` | GET | 集群成员列表 |
| `/cluster/grains` | GET | Grain 实例列表 |
| `/grain/call` | POST | Grain 调用 |

## 测试

### 单元测试

```bash
go test ./... -v
```

### 集成测试

```bash
go test ./tests/... -v
```

## 质量指标

- 单元测试覆盖率 >80%
- 单节点支持 1000+ 活跃 Grain
- Grain 调用延迟 <10ms (p99)
- 支持 1000+ QPS

