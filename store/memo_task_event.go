package store

import (
	"fmt"

	"orchestrator/task"
)

type InMemoryTaskEventStore struct {
	events map[string]*task.TaskEvent
}

func NewInMemoryTaskEventStore() *InMemoryTaskEventStore {
	return &InMemoryTaskEventStore{
		events: make(map[string]*task.TaskEvent),
	}
}

func (i *InMemoryTaskEventStore) Put(key string, value interface{}) error {
	e, ok := value.(*task.TaskEvent)

	if !ok {
		return fmt.Errorf("value %v is not a task.TaskEvent type", value)
	}

	i.events[key] = e

	return nil
}
func (i *InMemoryTaskEventStore) Get(key string) (interface{}, error) {
	e, ok := i.events[key]

	if !ok {
		return nil, fmt.Errorf("task event with key %s does not exist", key)
	}

	return e, nil
}
func (i *InMemoryTaskEventStore) List() (interface{}, error) {
	var events []*task.TaskEvent

	for _, e := range i.events {
		events = append(events, e)

	}

	return events, nil
}
func (i *InMemoryTaskEventStore) Count() (int, error) {
	return len(i.events), nil
}
