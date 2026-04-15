package app

import (
	"errors"
	"fmt"
	"os"
)

var errUsage = errors.New("usage")

func Run(args []string) int {
	cfg, err := parseConfig(args)
	if err != nil {
		if errors.Is(err, errUsage) {
			printUsage(os.Stderr)
			return ExitFailure
		}
		fmt.Fprintf(os.Stderr, "error: %v\n\n", err)
		printUsage(os.Stderr)
		return ExitFailure
	}

	var report Report
	switch cfg.Command {
	case "validate":
		report = runValidate(cfg)
	case "plan":
		report = runPlan(cfg)
	case "migrate":
		report = runMigrate(cfg)
	case "report":
		report, err = runReport(cfg)
		if err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			return ExitFailure
		}
	default:
		fmt.Fprintf(os.Stderr, "error: unsupported command %q\n", cfg.Command)
		return ExitFailure
	}

	if cfg.Command != "report" {
		if err := ensureOutputDir(cfg.OutputDir); err != nil {
			fmt.Fprintf(os.Stderr, "error: creating output directory: %v\n", err)
			return ExitFailure
		}
		if err := writeReport(report, cfg.ReportFormat, defaultOutputPath(cfg)); err != nil {
			fmt.Fprintf(os.Stderr, "error: writing report: %v\n", err)
			return ExitFailure
		}
	}

	printSummary(os.Stdout, report)
	return exitCodeFor(report)
}
