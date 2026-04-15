package config

type AlertingConfig struct {
	// MaxEnabledRules is the safety cap on the number of enabled alert rules loaded per evaluation cycle.
	MaxEnabledRules int `yaml:"max_enabled_rules"`
}

func (c Config) AlertingMaxEnabledRules() int {
	n := c.Alerting.MaxEnabledRules
	if n <= 0 {
		return 10000
	}
	return n
}
