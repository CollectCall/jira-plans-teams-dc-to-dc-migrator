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
	WeeklyHours *float64   `json:"weeklyHours,omitempty"`
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
	SourceResourceID int64    `json:"sourceResourceId"`
	SourceTeamID     int64    `json:"sourceTeamId"`
	SourceTeamName   string   `json:"sourceTeamName,omitempty"`
	SourcePersonID   int64    `json:"sourcePersonId"`
	SourceEmail      string   `json:"sourceEmail,omitempty"`
	TargetEmail      string   `json:"targetEmail,omitempty"`
	TargetTeamID     string   `json:"targetTeamId,omitempty"`
	TargetTeamName   string   `json:"targetTeamName,omitempty"`
	TargetUserID     string   `json:"targetUserId,omitempty"`
	WeeklyHours      *float64 `json:"weeklyHours,omitempty"`
	Status           string   `json:"status"`
	Reason           string   `json:"reason,omitempty"`
}

type JiraField struct {
	ID          string           `json:"id"`
	Name        string           `json:"name"`
	Custom      bool             `json:"custom"`
	Schema      *JiraFieldSchema `json:"schema,omitempty"`
	ClauseNames []string         `json:"clauseNames,omitempty"`
}

type JiraIssueType struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Subtask bool   `json:"subtask,omitempty"`
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

type JiraFilterSearchResults struct {
	StartAt    int          `json:"startAt"`
	MaxResults int          `json:"maxResults"`
	Total      int          `json:"total"`
	Values     []JiraFilter `json:"values"`
}

type JiraFilter struct {
	ID          string          `json:"id"`
	Name        string          `json:"name"`
	Description string          `json:"description,omitempty"`
	JQL         string          `json:"jql,omitempty"`
	Owner       *JiraFilterUser `json:"owner,omitempty"`
	ViewURL     string          `json:"viewUrl,omitempty"`
	SearchURL   string          `json:"searchUrl,omitempty"`
}

type JiraFilterUser struct {
	Name        string `json:"name,omitempty"`
	Key         string `json:"key,omitempty"`
	DisplayName string `json:"displayName,omitempty"`
}

type HierarchyLevelDTO struct {
	ID           int64    `json:"id"`
	Title        string   `json:"title"`
	IssueTypeIDs []string `json:"issueTypeIds"`
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
	ProjectName     string `json:"projectName,omitempty"`
	ProjectType     string `json:"projectType,omitempty"`
	Summary         string `json:"summary,omitempty"`
	TeamsFieldID    string `json:"teamsFieldId"`
	SourceTeamIDs   string `json:"sourceTeamIds"`
	SourceTeamNames string `json:"sourceTeamNames"`
}

type ParentLinkRow struct {
	IssueKey               string `json:"issueKey"`
	IssueID                string `json:"issueId,omitempty"`
	ProjectKey             string `json:"projectKey,omitempty"`
	ProjectName            string `json:"projectName,omitempty"`
	ProjectType            string `json:"projectType,omitempty"`
	Summary                string `json:"summary,omitempty"`
	ParentLinkFieldID      string `json:"parentLinkFieldId"`
	SourceParentIssueID    string `json:"sourceParentIssueId,omitempty"`
	SourceParentIssueKey   string `json:"sourceParentIssueKey,omitempty"`
	SourceParentSummary    string `json:"sourceParentSummary,omitempty"`
	SourceParentProjectKey string `json:"sourceParentProjectKey,omitempty"`
}

type ParentLinkFieldRow struct {
	FieldID      string `json:"fieldId"`
	FieldName    string `json:"fieldName"`
	SchemaCustom string `json:"schemaCustom,omitempty"`
	SchemaType   string `json:"schemaType,omitempty"`
}

type FilterTeamClauseRow struct {
	FilterID       string `json:"filterId"`
	FilterName     string `json:"filterName"`
	Owner          string `json:"owner,omitempty"`
	MatchType      string `json:"matchType"`
	ClauseValue    string `json:"clauseValue"`
	SourceTeamID   string `json:"sourceTeamId,omitempty"`
	SourceTeamName string `json:"sourceTeamName,omitempty"`
	Clause         string `json:"clause"`
	JQL            string `json:"jql"`
}

type TargetFilterSnapshotRow struct {
	TargetFilterID   string `json:"targetFilterId"`
	TargetFilterName string `json:"targetFilterName"`
	TargetOwner      string `json:"targetOwner,omitempty"`
	Description      string `json:"description,omitempty"`
	JQL              string `json:"jql,omitempty"`
	ViewURL          string `json:"viewUrl,omitempty"`
	SearchURL        string `json:"searchUrl,omitempty"`
}

type TargetIssueSnapshotRow struct {
	IssueKey             string `json:"issueKey"`
	ProjectKey           string `json:"projectKey,omitempty"`
	ProjectName          string `json:"projectName,omitempty"`
	ProjectType          string `json:"projectType,omitempty"`
	Summary              string `json:"summary,omitempty"`
	TargetTeamsFieldID   string `json:"targetTeamsFieldId,omitempty"`
	CurrentTargetTeamIDs string `json:"currentTargetTeamIds,omitempty"`
}

type TargetParentLinkSnapshotRow struct {
	IssueKey                string `json:"issueKey"`
	IssueID                 string `json:"issueId,omitempty"`
	ProjectKey              string `json:"projectKey,omitempty"`
	ProjectName             string `json:"projectName,omitempty"`
	ProjectType             string `json:"projectType,omitempty"`
	Summary                 string `json:"summary,omitempty"`
	TargetParentLinkFieldID string `json:"targetParentLinkFieldId,omitempty"`
	CurrentParentIssueID    string `json:"currentParentIssueId,omitempty"`
	CurrentParentIssueKey   string `json:"currentParentIssueKey,omitempty"`
}

type PostMigrationIssueComparisonRow struct {
	IssueKey             string `json:"issueKey"`
	ProjectKey           string `json:"projectKey,omitempty"`
	ProjectName          string `json:"projectName,omitempty"`
	ProjectType          string `json:"projectType,omitempty"`
	Summary              string `json:"summary,omitempty"`
	SourceTeamsFieldID   string `json:"sourceTeamsFieldId,omitempty"`
	TargetTeamsFieldID   string `json:"targetTeamsFieldId,omitempty"`
	SourceTeamIDs        string `json:"sourceTeamIds,omitempty"`
	SourceTeamNames      string `json:"sourceTeamNames,omitempty"`
	TargetTeamIDs        string `json:"targetTeamIds,omitempty"`
	CurrentTargetTeamIDs string `json:"currentTargetTeamIds,omitempty"`
	Status               string `json:"status"`
	Reason               string `json:"reason,omitempty"`
}

type PostMigrationIssueResultRow struct {
	IssueKey             string `json:"issueKey"`
	SourceTeamsFieldID   string `json:"sourceTeamsFieldId,omitempty"`
	TargetTeamsFieldID   string `json:"targetTeamsFieldId,omitempty"`
	SourceTeamIDs        string `json:"sourceTeamIds,omitempty"`
	TargetTeamIDs        string `json:"targetTeamIds,omitempty"`
	CurrentTargetTeamIDs string `json:"currentTargetTeamIds,omitempty"`
	Status               string `json:"status"`
	Message              string `json:"message,omitempty"`
}

type PostMigrationParentLinkComparisonRow struct {
	IssueKey                string `json:"issueKey"`
	IssueID                 string `json:"issueId,omitempty"`
	ProjectKey              string `json:"projectKey,omitempty"`
	ProjectName             string `json:"projectName,omitempty"`
	ProjectType             string `json:"projectType,omitempty"`
	Summary                 string `json:"summary,omitempty"`
	SourceParentLinkFieldID string `json:"sourceParentLinkFieldId,omitempty"`
	TargetParentLinkFieldID string `json:"targetParentLinkFieldId,omitempty"`
	SourceParentIssueID     string `json:"sourceParentIssueId,omitempty"`
	SourceParentIssueKey    string `json:"sourceParentIssueKey,omitempty"`
	TargetParentIssueID     string `json:"targetParentIssueId,omitempty"`
	TargetParentIssueKey    string `json:"targetParentIssueKey,omitempty"`
	CurrentParentIssueID    string `json:"currentParentIssueId,omitempty"`
	CurrentParentIssueKey   string `json:"currentParentIssueKey,omitempty"`
	Status                  string `json:"status"`
	Reason                  string `json:"reason,omitempty"`
}

type PostMigrationParentLinkResultRow struct {
	IssueKey                string `json:"issueKey"`
	SourceParentLinkFieldID string `json:"sourceParentLinkFieldId,omitempty"`
	TargetParentLinkFieldID string `json:"targetParentLinkFieldId,omitempty"`
	SourceParentIssueID     string `json:"sourceParentIssueId,omitempty"`
	SourceParentIssueKey    string `json:"sourceParentIssueKey,omitempty"`
	TargetParentIssueID     string `json:"targetParentIssueId,omitempty"`
	TargetParentIssueKey    string `json:"targetParentIssueKey,omitempty"`
	CurrentParentIssueID    string `json:"currentParentIssueId,omitempty"`
	CurrentParentIssueKey   string `json:"currentParentIssueKey,omitempty"`
	Status                  string `json:"status"`
	Message                 string `json:"message,omitempty"`
}

type PostMigrationFilterMatchRow struct {
	SourceFilterID   string `json:"sourceFilterId"`
	SourceFilterName string `json:"sourceFilterName"`
	SourceOwner      string `json:"sourceOwner,omitempty"`
	TargetFilterID   string `json:"targetFilterId,omitempty"`
	TargetFilterName string `json:"targetFilterName,omitempty"`
	TargetOwner      string `json:"targetOwner,omitempty"`
	Status           string `json:"status"`
	Reason           string `json:"reason,omitempty"`
}

type PostMigrationFilterComparisonRow struct {
	SourceFilterID     string `json:"sourceFilterId"`
	SourceFilterName   string `json:"sourceFilterName"`
	SourceOwner        string `json:"sourceOwner,omitempty"`
	SourceJQL          string `json:"sourceJql,omitempty"`
	SourceClause       string `json:"sourceClause,omitempty"`
	SourceTeamID       string `json:"sourceTeamId,omitempty"`
	TargetFilterID     string `json:"targetFilterId,omitempty"`
	TargetFilterName   string `json:"targetFilterName,omitempty"`
	TargetOwner        string `json:"targetOwner,omitempty"`
	TargetTeamID       string `json:"targetTeamId,omitempty"`
	CurrentTargetJQL   string `json:"currentTargetJql,omitempty"`
	RewrittenTargetJQL string `json:"rewrittenTargetJql,omitempty"`
	Status             string `json:"status"`
	Reason             string `json:"reason,omitempty"`
}

type PostMigrationFilterResultRow struct {
	SourceFilterID     string `json:"sourceFilterId,omitempty"`
	SourceFilterName   string `json:"sourceFilterName,omitempty"`
	SourceJQL          string `json:"sourceJql,omitempty"`
	TargetFilterID     string `json:"targetFilterId,omitempty"`
	TargetFilterName   string `json:"targetFilterName,omitempty"`
	CurrentTargetJQL   string `json:"currentTargetJql,omitempty"`
	RewrittenTargetJQL string `json:"rewrittenTargetJql,omitempty"`
	TargetJQLBefore    string `json:"targetJqlBefore,omitempty"`
	TargetJQLAfter     string `json:"targetJqlAfter,omitempty"`
	Status             string `json:"status"`
	Message            string `json:"message,omitempty"`
}
