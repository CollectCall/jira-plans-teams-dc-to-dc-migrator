package app

type TeamDTO struct {
	ID        int64  `json:"id"`
	Title     string `json:"title"`
	Shareable bool   `json:"shareable"`
	AvatarURL string `json:"avatarUrl,omitempty"`
}

type JiraUserDTO struct {
	JiraUserID   string `json:"jiraUserId,omitempty"`
	Title        string `json:"title,omitempty"`
	Email        string `json:"email,omitempty"`
	AvatarURL    string `json:"avatarUrl,omitempty"`
	JiraUsername string `json:"jiraUsername,omitempty"`
}

type PersonDTO struct {
	ID        int64        `json:"id"`
	Title     string       `json:"title,omitempty"`
	AvatarURL string       `json:"avatarUrl,omitempty"`
	JiraUser  *JiraUserDTO `json:"jiraUser,omitempty"`
}

type ResourceDTO struct {
	ID          int64      `json:"id"`
	WeeklyHours float64    `json:"weeklyHours,omitempty"`
	JoinDate    int64      `json:"joinDate,omitempty"`
	LeaveDate   int64      `json:"leaveDate,omitempty"`
	TeamID      int64      `json:"teamId"`
	Person      *PersonDTO `json:"person,omitempty"`
}

type ProgramDTO struct {
	ID          int64  `json:"id"`
	Title       string `json:"title"`
	Owner       string `json:"owner,omitempty"`
	Description string `json:"description,omitempty"`
}

type PlanDTO struct {
	ID                        int64   `json:"id"`
	PlanID                    int64   `json:"planId,omitempty"`
	Title                     string  `json:"title"`
	TimeZone                  string  `json:"timeZone,omitempty"`
	DefaultTeamWeeklyCapacity float64 `json:"defaultTeamWeeklyCapacity,omitempty"`
	HoursPerDay               float64 `json:"hoursPerDay,omitempty"`
	ProgramID                 int64   `json:"programId,omitempty"`
	PlanTeams                 []int64 `json:"planTeams,omitempty"`
}

type IdentityMapping map[string]string

type Artifact struct {
	Key   string `json:"key"`
	Label string `json:"label"`
	Path  string `json:"path"`
	Count int    `json:"count"`
}

type ProgramMapping struct {
	SourceProgramID int64  `json:"sourceProgramId"`
	SourceTitle     string `json:"sourceTitle"`
	SourceOwner     string `json:"sourceOwner,omitempty"`
	TargetProgramID string `json:"targetProgramId"`
	TargetTitle     string `json:"targetTitle"`
	Decision        string `json:"decision"`
	ConflictReason  string `json:"conflictReason,omitempty"`
}

type PlanMapping struct {
	SourcePlanID         int64  `json:"sourcePlanId"`
	SourceTitle          string `json:"sourceTitle"`
	SourceProgramID      int64  `json:"sourceProgramId,omitempty"`
	SourceProgramTitle   string `json:"sourceProgramTitle,omitempty"`
	SourcePlanTeamIDs    string `json:"sourcePlanTeamIds,omitempty"`
	SourcePlanTeamTitles string `json:"sourcePlanTeamTitles,omitempty"`
	TargetPlanID         string `json:"targetPlanId"`
	TargetTitle          string `json:"targetTitle"`
	TargetProgramID      string `json:"targetProgramId,omitempty"`
	TargetProgramTitle   string `json:"targetProgramTitle,omitempty"`
	TargetPlanTeamIDs    string `json:"targetPlanTeamIds,omitempty"`
	TargetPlanTeamTitles string `json:"targetPlanTeamTitles,omitempty"`
	Decision             string `json:"decision"`
	ConflictReason       string `json:"conflictReason,omitempty"`
}

type TeamMapping struct {
	SourceTeamID    int64  `json:"sourceTeamId"`
	SourceTitle     string `json:"sourceTitle"`
	SourceShareable bool   `json:"sourceShareable"`
	TargetTeamID    string `json:"targetTeamId"`
	TargetTitle     string `json:"targetTitle"`
	Decision        string `json:"decision"`
	Reason          string `json:"reason,omitempty"`
	ConflictReason  string `json:"conflictReason,omitempty"`
}

type ResourcePlan struct {
	SourceResourceID int64   `json:"sourceResourceId"`
	SourceTeamID     int64   `json:"sourceTeamId"`
	SourceTeamName   string  `json:"sourceTeamName,omitempty"`
	SourcePersonID   int64   `json:"sourcePersonId"`
	SourceEmail      string  `json:"sourceEmail,omitempty"`
	TargetEmail      string  `json:"targetEmail,omitempty"`
	TargetTeamID     string  `json:"targetTeamId,omitempty"`
	TargetTeamName   string  `json:"targetTeamName,omitempty"`
	TargetUserID     string  `json:"targetUserId,omitempty"`
	WeeklyHours      float64 `json:"weeklyHours"`
	Status           string  `json:"status"`
	Reason           string  `json:"reason,omitempty"`
}

type JiraField struct {
	ID          string           `json:"id"`
	Name        string           `json:"name"`
	Custom      bool             `json:"custom"`
	Schema      *JiraFieldSchema `json:"schema,omitempty"`
	ClauseNames []string         `json:"clauseNames,omitempty"`
}

type JiraFieldSchema struct {
	Type     string `json:"type,omitempty"`
	Items    string `json:"items,omitempty"`
	Custom   string `json:"custom,omitempty"`
	CustomID int64  `json:"customId,omitempty"`
}

type JiraSearchResults struct {
	StartAt    int         `json:"startAt"`
	MaxResults int         `json:"maxResults"`
	Total      int         `json:"total"`
	Issues     []JiraIssue `json:"issues"`
}

type JiraIssue struct {
	ID     string         `json:"id"`
	Key    string         `json:"key"`
	Fields map[string]any `json:"fields"`
}

type CoreJiraUser struct {
	Key          string `json:"key"`
	Name         string `json:"name"`
	EmailAddress string `json:"emailAddress"`
	DisplayName  string `json:"displayName"`
	Active       bool   `json:"active"`
}

type TeamsFieldSelection struct {
	Field      JiraField `json:"field"`
	Decision   string    `json:"decision"`
	Reason     string    `json:"reason,omitempty"`
	Candidates []string  `json:"candidates,omitempty"`
}

type IssueTeamRow struct {
	IssueKey        string `json:"issueKey"`
	ProjectKey      string `json:"projectKey,omitempty"`
	Summary         string `json:"summary,omitempty"`
	TeamsFieldID    string `json:"teamsFieldId"`
	TeamsFieldName  string `json:"teamsFieldName"`
	SourceTeamIDs   string `json:"sourceTeamIds"`
	SourceTeamNames string `json:"sourceTeamNames"`
}
