package gatekeeper

import (
	"fmt"
	"github.com/ernilsson/gatekeeper/internal/entity"
	"time"
)

type Principal struct {
	*entity.Entity
	// Subject can be used as an external identifier for the principal, such as a user identifier.
	Subject  string
	Created  time.Time
	LastSeen time.Time
}

type Namespace struct {
	*entity.Entity
	Name string
}

type Inheritance struct {
	Parent Relationship
	Child  Relationship
}

type Relationship struct {
	*entity.Entity
	Namespace Namespace
	Name      string
	Created   time.Time
	Updated   time.Time
}

func (r Relationship) QualifiedName() string {
	return fmt.Sprintf("%s:%s", r.Namespace.Name, r.Name)
}
