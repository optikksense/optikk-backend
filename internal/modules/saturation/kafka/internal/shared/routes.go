// Package shared holds HTTP-side helpers reused across all saturation/kafka
// submodules' handlers. SQL emission lives in the sibling
// `internal/modules/saturation/kafka/filter/` package.
package shared

import "github.com/gin-gonic/gin"

const SaturationRoutePrefix = "/saturation/kafka"

func RegisterGET(v1 *gin.RouterGroup, suffix string, handlers ...gin.HandlerFunc) {
	v1.GET(SaturationRoutePrefix+suffix, handlers...)
}

func RegisterGroup(v1 *gin.RouterGroup, suffix string, register func(group *gin.RouterGroup)) {
	register(v1.Group(SaturationRoutePrefix + suffix))
}
