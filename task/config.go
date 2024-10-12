package task

import "github.com/docker/go-connections/nat"

type Config struct {
	Name string

	AttachStdin  bool
	AttachStdout bool
	AttachStderr bool

	ExposedPorts nat.PortSet

	Cmd   []string
	Image string

	Cpu    float64
	Memory int64
	Disk   int64

	Env []string

	RestartPolicy string
}

func NewConfig(task *Task) *Config {
	return &Config{
		Name:          task.Name,
		Image:         task.Image,
		ExposedPorts:  task.ExposedPorts,
		Memory:        task.Memory,
		Disk:          task.Disk,
		RestartPolicy: task.RestartPolicy,
	}
}
