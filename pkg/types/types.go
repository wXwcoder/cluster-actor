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
