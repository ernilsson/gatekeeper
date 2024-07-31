package entity

import (
	"sync"
	"time"
)

func NewEvent(name string, payload interface{}) Event {
	return Event{
		Name:    name,
		Payload: payload,
		Created: time.Now(),
	}
}

type Event struct {
	Name    string      `json:"name"`
	Payload interface{} `json:"payload"`
	Version uint        `json:"Version"`
	Created time.Time   `json:"created"`
}

type Entity struct {
	mu *sync.Mutex

	ID      string
	events  []Event
	Version uint
}

func (e *Entity) Events() []Event {
	events := make([]Event, len(e.events))
	copy(events, e.events)
	return events
}

func (e *Entity) Raise(event Event) {
	e.mu.Lock()
	defer e.mu.Unlock()
	event.Version = e.Version + 1
	e.events = append(e.events, event)
	e.Version = event.Version
}
