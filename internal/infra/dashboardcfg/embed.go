package dashboardcfg

import "embed"

//go:embed pages/*/page.json pages/*/tabs/*.json
var files embed.FS

func Load() (*Registry, error) {
	return LoadFromFS(files)
}
