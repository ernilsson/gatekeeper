package gatekeeper

import "errors"

var (
	ErrNoExplicitPolicy = errors.New("no explicit policy")
	ErrNoOperationFound = errors.New("no operation found")
)
