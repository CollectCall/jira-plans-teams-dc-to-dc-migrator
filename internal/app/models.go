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

type IdentityMapping map[string]string

type TeamMapping struct {
	SourceTeamID    int64  `json:"sourceTeamId"`
	SourceTitle     string `json:"sourceTitle"`
	SourceShareable bool   `json:"sourceShareable"`
	TargetTeamID    string `json:"targetTeamId"`
	TargetTitle     string `json:"targetTitle"`
	Decision        string `json:"decision"`
	ConflictReason  string `json:"conflictReason,omitempty"`
}

type ResourcePlan struct {
	SourceResourceID int64   `json:"sourceResourceId"`
	SourceTeamID     int64   `json:"sourceTeamId"`
	SourcePersonID   int64   `json:"sourcePersonId"`
	TargetTeamID     string  `json:"targetTeamId,omitempty"`
	TargetUserID     string  `json:"targetUserId,omitempty"`
	WeeklyHours      float64 `json:"weeklyHours"`
	Status           string  `json:"status"`
	Reason           string  `json:"reason,omitempty"`
}
