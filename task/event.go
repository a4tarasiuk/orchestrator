package task

import (
	"github.com/google/uuid"
	"time"
)

type TaskEvent struct {
	ID        uuid.UUID
	State     State
	Timestamp time.Time
	Task      Task
}
