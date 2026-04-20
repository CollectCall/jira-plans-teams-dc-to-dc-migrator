package app

import "strings"

const (
	phasePreMigrate  = "pre-migrate"
	phaseMigrate     = "migrate"
	phasePostMigrate = "post-migrate"
)

func defaultMigrationPhase(command string) string {
	if command == "migrate" {
		return phaseMigrate
	}
	return ""
}

func normalizeMigrationPhase(value string) string {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case phasePreMigrate:
		return phasePreMigrate
	case phaseMigrate:
		return phaseMigrate
	case phasePostMigrate:
		return phasePostMigrate
	default:
		return ""
	}
}

func reportPhase(report Report) string {
	if normalized := normalizeMigrationPhase(report.Phase); normalized != "" {
		return normalized
	}
	return defaultMigrationPhase(report.Command)
}

func runsPreMigratePhase(command, phase string) bool {
	return command == "migrate" && normalizeMigrationPhase(phase) == phasePreMigrate
}

func runsMigratePhase(command, phase string) bool {
	return command == "migrate" && normalizeMigrationPhase(phase) == phaseMigrate
}

func runsPostMigratePhase(command, phase string) bool {
	return command == "migrate" && normalizeMigrationPhase(phase) == phasePostMigrate
}

func availableMigrationPhases(cfg Config) []string {
	choices := []string{phasePreMigrate, phaseMigrate}
	if strings.TrimSpace(cfg.TargetBaseURL) != "" {
		choices = append(choices, phasePostMigrate)
	}
	return choices
}
