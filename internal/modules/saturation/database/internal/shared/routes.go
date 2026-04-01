package shared

import "github.com/gin-gonic/gin"

const (
	SaturationRoutePrefix = "/saturation/database"
	LegacyRoutePrefix     = "/database"
)

func RegisterDualGET(v1 *gin.RouterGroup, suffix string, handlers ...gin.HandlerFunc) {
	v1.GET(SaturationRoutePrefix+suffix, handlers...)
	v1.GET(LegacyRoutePrefix+suffix, handlers...)
}

func RegisterDualGroup(v1 *gin.RouterGroup, suffix string, register func(group *gin.RouterGroup)) {
	register(v1.Group(SaturationRoutePrefix + suffix))
	register(v1.Group(LegacyRoutePrefix + suffix))
}
