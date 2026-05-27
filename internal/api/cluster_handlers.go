package api

import (
	"net/http"
	"time"

	"github.com/gin-gonic/gin"
)

// getClusterInfo 获取集群信息
func (r *Router) getClusterInfo(c *gin.Context) {
	response := gin.H{
		"cluster_name": r.cfg.ClusterName,
		"node_name":    r.cfg.NodeName,
		"node_address": r.cfg.Host,
		"member_count": r.memberCount(),
		"is_running":   r.isRunning(),
		"start_time":   time.Now().Format(time.RFC3339),
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 0,
		"data": response,
	})
}

// getClusterMembers 获取集群成员列表
func (r *Router) getClusterMembers(c *gin.Context) {
	members, err := r.getMembers()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    -1,
			"message": "获取集群成员失败: " + err.Error(),
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"code":  0,
		"count": len(members),
		"data":  members,
	})
}

// getClusterStatus 获取集群状态
func (r *Router) getClusterStatus(c *gin.Context) {
	members, err := r.getMembers()
	if err != nil {
		c.JSON(http.StatusInternalServerError, gin.H{
			"code":    -1,
			"message": "获取集群状态失败: " + err.Error(),
		})
		return
	}

	aliveCount := 0
	for _, m := range members {
		if m.Alive {
			aliveCount++
		}
	}

	status := "healthy"
	if aliveCount == 0 {
		status = "unhealthy"
	} else if aliveCount < len(members) {
		status = "degraded"
	}

	response := gin.H{
		"status":       status,
		"member_count": len(members),
		"members":      members,
		"timestamp":    time.Now().Format(time.RFC3339),
	}

	c.JSON(http.StatusOK, gin.H{
		"code": 0,
		"data": response,
	})
}
