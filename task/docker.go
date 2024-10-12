package task

import (
	"context"
	"io"
	"log"
	"math"
	"os"

	"github.com/docker/docker/api/types"
	"github.com/docker/docker/api/types/container"
	"github.com/docker/docker/api/types/image"
	"github.com/docker/docker/client"
	"github.com/docker/docker/pkg/stdcopy"
)

type DockerContainer struct {
	Client      *client.Client
	Config      Config
	ContainerID string
}

func NewDockerContainer(config *Config) *DockerContainer {
	dockerClient, _ := client.NewClientWithOpts(client.FromEnv)

	return &DockerContainer{Client: dockerClient, Config: *config}
}

func (d *DockerContainer) Run() DockerResult {
	ctx := context.Background()

	reader, err := d.Client.ImagePull(ctx, d.Config.Image, image.PullOptions{})

	if err != nil {
		log.Printf("Error pulling image %s: %v\n", d.Config.Image, err)

		return DockerResult{Error: err}
	}

	io.Copy(os.Stdout, reader)

	restartPolicy := container.RestartPolicy{Name: container.RestartPolicyMode(d.Config.RestartPolicy)}

	resources := container.Resources{Memory: d.Config.Memory, NanoCPUs: int64(d.Config.Cpu * math.Pow(10, 9))}

	containerConfig := container.Config{
		Image:        d.Config.Image,
		Tty:          false,
		Env:          d.Config.Env,
		ExposedPorts: d.Config.ExposedPorts,
	}

	hostConfig := container.HostConfig{
		RestartPolicy:   restartPolicy,
		Resources:       resources,
		PublishAllPorts: true,
	}

	response, err := d.Client.ContainerCreate(ctx, &containerConfig, &hostConfig, nil, nil, d.Config.Name)

	if err != nil {
		log.Printf("Error creating container %s: %v\n", d.Config.Image, err)

		return DockerResult{Error: err}
	}

	err = d.Client.ContainerStart(ctx, response.ID, container.StartOptions{})

	if err != nil {
		log.Printf("Error starting container %s: %v\n", d.Config.Image, err)

		return DockerResult{Error: err}
	}

	d.ContainerID = response.ID

	out, err := d.Client.ContainerLogs(ctx, d.ContainerID, container.LogsOptions{ShowStdout: true, ShowStderr: true})

	if err != nil {
		log.Printf("Error getting container logs: %v\n", err)

		return DockerResult{Error: err}
	}

	stdcopy.StdCopy(os.Stdout, os.Stderr, out)

	return DockerResult{ContainerID: d.ContainerID, Action: "start", Result: "success"}
}

func (d *DockerContainer) Stop(id string) DockerResult {
	log.Printf("Attempting to stop container %v", id)

	ctx := context.Background()

	err := d.Client.ContainerStop(ctx, id, container.StopOptions{})

	if err != nil {
		log.Printf("Error stopping container %s: %v\n", id, err)

		return DockerResult{Error: err}
	}

	err = d.Client.ContainerRemove(
		ctx, id, container.RemoveOptions{
			RemoveVolumes: true,
			RemoveLinks:   false,
			Force:         false,
		},
	)

	if err != nil {
		log.Printf("Error removing container %s: %v\n", id, err)

		return DockerResult{Error: err}
	}

	return DockerResult{Action: "stop", Result: "success", Error: nil}
}

func (d *DockerContainer) Inspect(id string) DockerInspectResponse {
	ctx := context.Background()

	resp, err := d.Client.ContainerInspect(ctx, id)

	if err != nil {
		log.Printf("Error inspecting container: %s\n", err)

		return DockerInspectResponse{Error: err}
	}

	return DockerInspectResponse{Container: &resp}
}

type DockerResult struct {
	Error       error
	Action      string
	ContainerID string
	Result      string
}

type DockerInspectResponse struct {
	Error     error
	Container *types.ContainerJSON
}
