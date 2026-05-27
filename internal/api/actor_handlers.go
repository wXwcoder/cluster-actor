// Package api 提供HTTP API路由和处理
package api

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/asynkron/protoactor-go/cluster"
	"github.com/cluster-actor/server/gen"
	"github.com/gin-gonic/gin"
)

// ActorInstanceInfo Actor实例信息
type ActorInstanceInfo struct {
	KindName    string `json:"kind_name"`
	Identity    string `json:"identity"`
	NodeName    string `json:"node_name"`
	NodeAddress string `json:"node_address"`
}

// getActorInstances 获取所有Actor实例列表
// 返回集群中所有节点的Actor信息
// 注意：当前实现返回当前节点的本地实例，跨节点获取需要通过其他方式实现
func (r *Router) getActorInstances(c *gin.Context) {
	// 获取集群成员列表
	members, err := r.getMembers()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    -1,
			"message": fmt.Sprintf("获取集群成员失败: %v", err),
		})
		return
	}

	// 收集所有节点的实例
	allInstances := make([]ActorInstanceInfo, 0)

	for _, member := range members {
		// 尝试从每个节点获取本地Grain实例
		// 使用member.Address作为HTTP API地址（需要在配置中确保Address包含HTTP端口）
		nodeInstances, err := r.fetchNodeInstances(member.Address, member.NodeName)
		if err != nil {
			// 如果获取失败，跳过该节点
			continue
		}
		allInstances = append(allInstances, nodeInstances...)
	}

	c.JSON(http.StatusOK, gin.H{
		"code":  0,
		"count": len(allInstances),
		"data":  allInstances,
	})
}

// fetchNodeInstances 从指定节点获取其本地Grain实例列表
func (r *Router) fetchNodeInstances(nodeAddress string, nodeName string) ([]ActorInstanceInfo, error) {
	// 从gRPC地址推导HTTP API地址
	// nodeAddress 格式: "host:grpc_port" (例如 "127.0.0.1:8081")
	// HTTP API端口 = gRPC端口 + 100 (例如 8181)
	httpAddress := grpcToHTTPAddress(nodeAddress)
	apiURL := fmt.Sprintf("http://%s/api/actor/local-instances", httpAddress)

	client := &http.Client{Timeout: time.Second * 3}
	resp, err := client.Get(apiURL)
	if err != nil {
		return nil, fmt.Errorf("获取节点[%s]实例失败: %v", nodeName, err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("获取节点[%s]实例失败，状态码: %d", nodeName, resp.StatusCode)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("读取节点[%s]响应失败: %v", nodeName, err)
	}

	var result struct {
		Code  int                 `json:"code"`
		Count int                 `json:"count"`
		Data  []ActorInstanceInfo `json:"data"`
	}

	if err := json.Unmarshal(body, &result); err != nil {
		return nil, fmt.Errorf("解析节点[%s]响应失败: %v", nodeName, err)
	}

	if result.Code != 0 {
		return nil, fmt.Errorf("节点[%s]返回错误: %v", nodeName, result)
	}

	// 确保每个实例都包含正确的节点名称
	for i := range result.Data {
		result.Data[i].NodeName = nodeName
	}

	return result.Data, nil
}

// grpcToHTTPAddress 将gRPC地址转换为HTTP API地址
// 规则: HTTP端口 = gRPC端口 + 100
// 例如: "127.0.0.1:8081" -> "127.0.0.1:8181"
func grpcToHTTPAddress(grpcAddr string) string {
	// 解析地址
	lastColon := strings.LastIndex(grpcAddr, ":")
	if lastColon == -1 {
		return grpcAddr + ":8180" // 默认端口
	}

	host := grpcAddr[:lastColon]
	grpcPortStr := grpcAddr[lastColon+1:]

	// 尝试解析端口
	var grpcPort int
	fmt.Sscanf(grpcPortStr, "%d", &grpcPort)

	if grpcPort == 0 {
		return grpcAddr + ":8180"
	}

	// HTTP端口 = gRPC端口 + 100
	httpPort := grpcPort + 100
	return fmt.Sprintf("%s:%d", host, httpPort)
}

// getLocalActorInstances 获取当前节点的本地Actor实例列表
func (r *Router) getLocalActorInstances(c *gin.Context) {
	if r.getLocalInstances == nil {
		c.JSON(http.StatusOK, gin.H{
			"code":  0,
			"count": 0,
			"data":  []ActorInstanceInfo{},
		})
		return
	}

	instances := r.getLocalInstances()

	result := make([]ActorInstanceInfo, 0, len(instances))
	for _, inst := range instances {
		result = append(result, ActorInstanceInfo{
			KindName:    inst.KindName,
			Identity:    inst.Identity,
			NodeName:    r.nodeName,
			NodeAddress: fmt.Sprintf("%s:%d", r.cfg.Host, r.cfg.Port),
		})
	}

	log.Printf("node[%s] instances: %v", r.nodeName, result)
	c.JSON(http.StatusOK, gin.H{
		"code":  0,
		"count": len(result),
		"data":  result,
	})
}

// getGrainKinds 获取所有Grain类型列表
func (r *Router) getGrainKinds(c *gin.Context) {
	// 使用真实数据替代硬编码
	if r.getKindNames == nil {
		c.JSON(http.StatusOK, gin.H{
			"code":  0,
			"count": 0,
			"data":  []gin.H{},
		})
		return
	}

	kindNames := r.getKindNames()
	kinds := make([]gin.H, 0, len(kindNames))
	for _, kindName := range kindNames {
		kinds = append(kinds, gin.H{
			"kind_name": kindName,
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"code":  0,
		"count": len(kinds),
		"data":  kinds,
	})
}

// testGrainCall 测试Grain调用（HTTP GET接口）
// 使用 protoactor cluster.RequestFuture 实现跨节点路由
// 当调用此接口时，框架会根据 identity 的哈希值自动路由到正确的节点
func (r *Router) GrainCall(c *gin.Context) {
	kindName := c.Param("kind")
	identity := c.Param("identity")

	// 获取查询参数
	name := c.DefaultQuery("name", "")
	if name == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    -1,
			"message": "缺少name参数",
		})
		return
	}

	// 通过 cluster.RequestFuture 发送请求到 Grain
	// 框架使用 DistHash 算法计算目标节点：
	// - 如果目标节点是当前节点，本地激活
	// - 如果目标节点是其他节点，通过 gRPC 远程激活和调用
	// 注意：必须使用 proto.Message 指针类型，否则远程调用会序列化失败
	// 参数顺序: identity, kind, message
	future, err := r.cluster.RequestFuture(identity, kindName, &gen.RpcReq{Kind: kindName, Identity: identity, Name: name}, cluster.WithTimeout(time.Second*5))
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    -1,
			"message": fmt.Sprintf("获取Grain失败: %v", err),
		})
		return
	}

	result, err := future.Result()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    -1,
			"message": fmt.Sprintf("调用Grain失败: %v", err),
		})
		return
	}

	// 处理响应（proto 消息为指针类型）
	if resp, ok := result.(*gen.RpcResp); ok {
		c.JSON(http.StatusOK, gin.H{
			"code": 0,
			"data": gin.H{
				"kind_name":  kindName,
				"identity":   identity,
				"result":     resp.Message,
				"call_count": resp.CallCount,
			},
		})
		return
	}

	c.JSON(http.StatusBadRequest, gin.H{
		"code":    -1,
		"message": "未知的响应类型",
	})
}
