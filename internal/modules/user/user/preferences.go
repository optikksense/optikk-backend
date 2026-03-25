package userpage

type UserPreferences struct {
	Theme                string   `json:"theme,omitempty"`
	Timezone             string   `json:"timezone,omitempty"`
	RefreshInterval      int      `json:"refreshInterval,omitempty"`
	SidebarCollapsed     bool     `json:"sidebarCollapsed,omitempty"`
	Density              string   `json:"density,omitempty"`
	NotificationsEnabled bool     `json:"notificationsEnabled,omitempty"`
	Favorites            []string `json:"favorites,omitempty"`
	DefaultTimeRange     string   `json:"defaultTimeRange,omitempty"`
	DefaultPageSize      int      `json:"defaultPageSize,omitempty"`
}

type PreferencesResponse struct {
	Preferences UserPreferences `json:"preferences"`
}

func defaultUserPreferences() UserPreferences {
	return UserPreferences{}
}
