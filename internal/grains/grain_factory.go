// Package grains 提供基于 protoactor-go 的虚拟 Actor 定义和实现
package grains

import (
	"github.com/asynkron/protoactor-go/actor"
)

// NewHelloGrainProps 创建 HelloGrain 的 Props 配置
// 返回值是可用于 spawn 的 Props 配置
// Grain 的 identity 由 protoactor 集群框架在激活时自动设置
func NewHelloGrainProps(identity string) *actor.Props {
	return actor.PropsFromProducer(func() actor.Actor {
		return &HelloGrain{BaseGrain{Identity: identity}}
	})
}
