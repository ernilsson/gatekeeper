package gatekeeper

type PrincipalEnrolled struct {
	GroupID     string `json:"group_id"`
	PrincipalID string `json:"principal_id"`
}

type PolicyAllowed struct {
	PolicyID string `json:"policy_id"`
}

type PolicyDenied struct {
	PolicyID string `json:"policy_id"`
}
