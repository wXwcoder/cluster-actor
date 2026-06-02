// Package api 提供KV Store相关的HTTP API处理器
package api

import (
	"net/http"
	"time"

	"github.com/cluster-actor/server/internal/kvstore"
	"github.com/gin-gonic/gin"
)

// getKVStoreStatus 获取KV存储状态和key列表
func (r *Router) getKVStoreStatus(c *gin.Context) {
	if r.kvStore == nil {
		c.JSON(http.StatusOK, gin.H{
			"code":    0,
			"data":    r.buildKVStoreStatus(nil, 0, "service_unavailable"),
			"message": "KVStore未初始化",
		})
		return
	}

	entries := r.kvStore.ListFromCache("")
	keyCount := len(entries)

	health := "healthy"
	if keyCount == 0 {
		health = "warning"
	}

	statusData := r.buildKVStoreStatus(entries, keyCount, health)

	c.JSON(http.StatusOK, gin.H{
		"code":  0,
		"data":  statusData,
		"count": keyCount,
	})
}

// getKVStoreKey 获取指定key的值
func (r *Router) getKVStoreKey(c *gin.Context) {
	if r.kvStore == nil {
		c.JSON(http.StatusOK, gin.H{
			"code":    -1,
			"message": "KVStore未初始化",
		})
		return
	}

	key := c.Param("key")
	if key == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"code":    -1,
			"message": "缺少key参数",
		})
		return
	}

	entry, found := r.kvStore.GetEntry(key)
	if !found {
		c.JSON(http.StatusOK, gin.H{
			"code":    -1,
			"message": "key不存在",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 0,
		"data": gin.H{
			"key":          entry.Key,
			"value":        string(entry.Value),
			"modify_index": entry.ModifyIndex,
			"created_at":   entry.CreatedAt.Format(time.RFC3339),
			"updated_at":   entry.UpdatedAt.Format(time.RFC3339),
		},
	})
}

// buildKVStoreStatus 构建KV存储状态响应数据
func (r *Router) buildKVStoreStatus(entries []*kvstore.KVEntry, keyCount int, health string) gin.H {
	var lastUpdate string
	if len(entries) > 0 {
		latestTime := entries[0].UpdatedAt
		for _, e := range entries {
			if e.UpdatedAt.After(latestTime) {
				latestTime = e.UpdatedAt
			}
		}
		lastUpdate = latestTime.Format(time.RFC3339)
	}

	keys := make([]gin.H, 0, len(entries))
	for _, e := range entries {
		keys = append(keys, gin.H{
			"key":         e.Key,
			"value":       string(e.Value),
			"length":      len(e.Value),
			"modify_index": e.ModifyIndex,
			"updated_at":  e.UpdatedAt.Format(time.RFC3339),
		})
	}

	return gin.H{
		"key_count":     keyCount,
		"health":        health,
		"usage_percent": r.calculateUsagePercent(keyCount),
		"last_update":   lastUpdate,
		"keys":          keys,
	}
}

// calculateUsagePercent 计算使用率百分比
func (r *Router) calculateUsagePercent(keyCount int) int {
	const maxKeys = 10000
	if keyCount == 0 {
		return 0
	}
	percent := (keyCount * 100) / maxKeys
	if percent > 100 {
		return 100
	}
	return percent
}

// GetAllKeys 获取所有key列表
func (r *Router) GetAllKeys(c *gin.Context) {
	if r.kvStore == nil {
		c.JSON(http.StatusOK, gin.H{
			"code":    -1,
			"message": "KVStore未初始化",
		})
		return
	}

	entries := r.kvStore.ListFromCache("")

	keys := make([]gin.H, 0, len(entries))
	for _, e := range entries {
		keys = append(keys, gin.H{
			"key":         e.Key,
			"value":       string(e.Value),
			"length":      len(e.Value),
			"modify_index": e.ModifyIndex,
			"updated_at":  e.UpdatedAt.Format(time.RFC3339),
		})
	}

	c.JSON(http.StatusOK, gin.H{
		"code":  0,
		"data":  keys,
		"count": len(keys),
	})
}
