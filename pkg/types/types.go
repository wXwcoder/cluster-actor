// Package types 提供共享类型定义
package types

import "time"

// MemberInfo 集群成员信息
type MemberInfo struct {
	NodeName  string    `json:"node_name"`
	Address   string    `json:"address"`
	Port      int       `json:"port"`
	KindNames []string  `json:"kind_names"`
	Alive     bool      `json:"alive"`
	LastSeen  time.Time `json:"last_seen"`
}

// KindType 集群成员类型定义
type KindType string

// KindTypeChat 聊天成员类型

const (
	Kind_Hello    KindType = "hello"
	Kind_User     KindType = "user"
	Kind_Chat     KindType = "chat"
	Kind_WsClient KindType = "client"
)
