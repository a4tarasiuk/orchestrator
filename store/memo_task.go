package store

import (
	"fmt"

	"orchestrator/task"
)

type InMemoryTaskStore struct {
	tasks map[string]*task.Task
}

func NewInMemoryTaskStore() *InMemoryTaskStore {
	return &InMemoryTaskStore{tasks: make(map[string]*task.Task)}
}

func (s *InMemoryTaskStore) Put(key string, value interface{}) error {
	t, ok := value.(*task.Task)

	if !ok {
		return fmt.Errorf("value %v is not a task.Task type", value)
	}

	s.tasks[key] = t

	return nil
}

func (s *InMemoryTaskStore) Get(key string) (interface{}, error) {
	t, ok := s.tasks[key]

	if !ok {
		return nil, fmt.Errorf("task with key %s does not exist", key)
	}

	return t, nil
}

func (s *InMemoryTaskStore) List() (interface{}, error) {
	var tasks []*task.Task

	for _, t := range s.tasks {
		tasks = append(tasks, t)
	}

	return tasks, nil
}

func (s *InMemoryTaskStore) Count() (int, error) {
	return len(s.tasks), nil
}
