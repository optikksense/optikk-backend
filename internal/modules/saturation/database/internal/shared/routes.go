package shared

import "github.com/gin-gonic/gin"

const SaturationRoutePrefix = "/saturation/database"

func RegisterGET(v1 *gin.RouterGroup, suffix string, handlers ...gin.HandlerFunc) {
	v1.GET(SaturationRoutePrefix+suffix, handlers...)
}

func RegisterGroup(v1 *gin.RouterGroup, suffix string, register func(group *gin.RouterGroup)) {
	register(v1.Group(SaturationRoutePrefix + suffix))
}
