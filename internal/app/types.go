package app

import "time"

const (
	ExitSuccess     = 0
	ExitFailure     = 1
	ExitStrictIssue = 2
)

type Severity string

const (
	SeverityInfo    Severity = "info"
	SeverityWarning Severity = "warning"
	SeverityError   Severity = "error"
)

type ReportFormat string

const (
	ReportFormatJSON ReportFormat = "json"
	ReportFormatCSV  ReportFormat = "csv"
)

type Report struct {
	Command      string         `json:"command"`
	DryRun       bool           `json:"dryRun"`
	Strict       bool           `json:"strict"`
	GeneratedAt  time.Time      `json:"generatedAt"`
	Source       Endpoint       `json:"source"`
	Target       Endpoint       `json:"target"`
	Inputs       InputFiles     `json:"inputs"`
	Actions      []Action       `json:"actions"`
	Findings     []Finding      `json:"findings"`
	Stats        ReportStats    `json:"stats"`
	ExitBehavior ExitBehavior   `json:"exitBehavior"`
	Metadata     map[string]any `json:"metadata,omitempty"`
}

type Endpoint struct {
	BaseURL string `json:"baseUrl"`
	Mode    string `json:"mode"`
}

type InputFiles struct {
	IdentityMapping string `json:"identityMapping,omitempty"`
	Teams           string `json:"teams,omitempty"`
	Persons         string `json:"persons,omitempty"`
	Resources       string `json:"resources,omitempty"`
	IssuesCSV       string `json:"issuesCsv,omitempty"`
}

type Action struct {
	Kind     string `json:"kind"`
	SourceID string `json:"sourceId,omitempty"`
	TargetID string `json:"targetId,omitempty"`
	Status   string `json:"status"`
	Details  string `json:"details,omitempty"`
}

type Finding struct {
	Severity Severity `json:"severity"`
	Code     string   `json:"code"`
	Message  string   `json:"message"`
}

type ReportStats struct {
	Infos    int `json:"infos"`
	Warnings int `json:"warnings"`
	Errors   int `json:"errors"`
	Actions  int `json:"actions"`
}

type ExitBehavior struct {
	SuccessCode          int  `json:"successCode"`
	FatalErrorCode       int  `json:"fatalErrorCode"`
	StrictIssueCode      int  `json:"strictIssueCode"`
	StrictIssuesDetected bool `json:"strictIssuesDetected"`
}
