package dashboardconfig

import "sync"

// DefaultConfigs maps pageId to its default YAML chart configuration.
var DefaultConfigs = map[string]string{}
var mu sync.RWMutex

// RegisterDefaultConfig allows other modules to register their default dashboards.
// It is safe for concurrent use, though typically called from init() functions.
func RegisterDefaultConfig(pageID string, config string) {
	mu.Lock()
	defer mu.Unlock()
	DefaultConfigs[pageID] = config
}

// GetDefaultConfig retrieves a registered default configuration.
func GetDefaultConfig(pageID string) (string, bool) {
	mu.RLock()
	defer mu.RUnlock()
	cfg, ok := DefaultConfigs[pageID]
	return cfg, ok
}

// GetAllDefaultConfigs returns a copy of all registered configurations.
func GetAllDefaultConfigs() map[string]string {
	mu.RLock()
	defer mu.RUnlock()

	copy := make(map[string]string, len(DefaultConfigs))
	for k, v := range DefaultConfigs {
		copy[k] = v
	}
	return copy
}
