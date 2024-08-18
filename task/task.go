package task

import (
	"github.com/docker/go-connections/nat"
	"github.com/google/uuid"
	"time"
)

type Task struct {
	ID          uuid.UUID
	ContainerID string

	Name  string
	State State

	Image string

	Memory int64
	Disk   int64

	ExposedPorts  nat.PortSet
	PortBindings  map[string]string
	RestartPolicy string

	StartTime  time.Time
	FinishTime time.Time
}
