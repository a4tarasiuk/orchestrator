package manager

import (
	"time"

	"github.com/google/uuid"
	"orchestrator/task"
)

type (
	WorkerAPIClient interface {
		StartTask(workerID uuid.UUID, taskEvent task.TaskEvent) (*task.Task, error)

		StopTask(workerHostPort string, taskID uuid.UUID) error

		GetTask(workerID uuid.UUID, taskID uuid.UUID) (*task.Task, error)

		GetTaskHealth(healthRequest WorkerTaskHealthRequest) (*WorkerTaskHealthResult, error)
	}

	WorkerTaskHealthRequest struct {
		TaskID uuid.UUID

		Host string
		Port string
		Path string
	}

	WorkerTaskHealthResult struct {
		WorkerID uuid.UUID

		TaskID uuid.UUID

		IsHealthy bool

		OccurredAt time.Time
	}
)
