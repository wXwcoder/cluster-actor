package api

import (
	"net/http"

	"github.com/gin-gonic/gin"
)

// healthCheck 健康检查处理器
func (r *Router) healthCheck(c *gin.Context) {
	if !r.isRunning() {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"status":   "unhealthy",
			"node_name": r.cfg.NodeName,
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"status":   "healthy",
		"node_name": r.cfg.NodeName,
	})
}
