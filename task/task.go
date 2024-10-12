package task

import (
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/google/uuid"
)

type Task struct {
	ID          uuid.UUID
	ContainerID string

	Name  string
	State State

	Image string

	Cpu    float64
	Memory int64
	Disk   int64

	ExposedPorts nat.PortSet
	HostPorts    nat.PortMap
	PortBindings map[string]string

	HealthCheck   string
	RestartCount  int
	RestartPolicy string

	StartTime  time.Time
	FinishTime time.Time
}
