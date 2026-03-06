package defaultconfig

import "embed"

//go:embed pages/*/page.json pages/*/tabs/*.json
var files embed.FS

// Load loads the embedded default configuration registry.
func Load() (*Registry, error) {
	return LoadFromFS(files)
}
