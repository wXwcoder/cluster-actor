package global

import (
	"fmt"
	"log"
	"time"

	"github.com/asynkron/protoactor-go/cluster"
	"github.com/cluster-actor/server/gen"
	"github.com/cluster-actor/server/internal/config"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/reflect/protoregistry"
)

type GlobalRegistry struct {
	Cfg     *config.ClusterConfig
	Cluster *cluster.Cluster
}

var G = &GlobalRegistry{}

func Rpc(identity string, kind string, name string, req proto.Message) (proto.Message, error) {
	if identity == "" || kind == "" || name == "" {
		return nil, fmt.Errorf("缺少identity或kind或name参数")
	}
	msgName := req.ProtoReflect().Descriptor().Name()
	msgId, ok := gen.MsgId_value["P"+string(msgName)]
	if !ok || msgId == 0 {
		return nil, fmt.Errorf("未知的消息类型: %s", msgName)
	}
	log.Printf("Rpc0 identity:%s kind:%s name:%s msgName:%s msgId:%d req:%+v", identity, kind, name, msgName, msgId, req)

	data, err := proto.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("序列化请求失败: %v", err)
	}
	resp, err := RequestFuture(&gen.RpcMsg{Kind: kind, Identity: identity, Name: name, MsgId: msgId, Data: data})
	if err != nil || resp == nil {
		return nil, fmt.Errorf("调用Grain失败: %v", err)
	}

	// 反序列化响应
	respName, ok := gen.MsgId_name[resp.MsgId]
	if !ok || respName == "" {
		return nil, fmt.Errorf("未知的消息类型: %d", msgId)
	}

	if resp.Code != int32(gen.ErrorCode_OK) || resp.Data == nil {
		return nil, fmt.Errorf("系统错误: %d", resp.Code)
	}
	//移除第一个字符P
	respName = "gen." + respName[1:]
	msgType, err := protoregistry.GlobalTypes.FindMessageByName(protoreflect.FullName(respName))
	if err != nil {
		return nil, fmt.Errorf("message type %s not found: %w", respName, err)
	}

	// 2. 创建该消息类型的一个新实例
	msg := msgType.New().Interface()
	if err := proto.Unmarshal(resp.Data, msg); err != nil {
		return nil, fmt.Errorf("反序列化响应失败: %v", err)
	}
	return msg.(proto.Message), nil
}

func RequestFuture(req *gen.RpcMsg) (*gen.RpcMsg, error) {
	// 获取查询参数
	name := req.Name
	if req == nil || name == "" || req.Kind == "" || req.Identity == "" {
		return nil, fmt.Errorf("缺少name或kind或identity参数")
	}

	log.Printf("RequestFuture: %+v", req)

	// 通过 cluster.RequestFuture 发送请求到 Grain
	// 框架使用 DistHash 算法计算目标节点：
	// - 如果目标节点是当前节点，本地激活
	// - 如果目标节点是其他节点，通过 gRPC 远程激活和调用
	// 注意：必须使用 proto.Message 指针类型，否则远程调用会序列化失败
	// 参数顺序: identity, kind, message
	future, err := G.Cluster.RequestFuture(req.Identity, req.Kind, req, cluster.WithTimeout(time.Second*5))
	if err != nil {
		return nil, fmt.Errorf("获取Grain失败: %v", err)
	}

	var result interface{}
	result, err = future.Result()
	if err != nil {
		return nil, fmt.Errorf("调用Grain失败: %v", err)
	}

	log.Printf("RequestFuture: %+v", result.(*gen.RpcMsg))
	// 处理响应（proto 消息为指针类型）
	if resp, ok := result.(*gen.RpcMsg); ok {
		return resp, nil
	} else {
		return nil, fmt.Errorf("未知的响应类型: %T", result)
	}
}
