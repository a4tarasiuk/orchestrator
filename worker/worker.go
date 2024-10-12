package worker

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
	"orchestrator/task"
)

type Worker struct {
	Name      string
	Queue     queue.Queue
	Db        map[uuid.UUID]*task.Task
	TaskCount int

	Stats *Stats
}

func (w *Worker) runTask() task.DockerResult {
	t := w.Queue.Dequeue()

	if t == nil {
		log.Println("No tasks in the queue")

		return task.DockerResult{Error: nil}
	}

	taskQueued := t.(task.Task)

	taskPersisted := w.Db[taskQueued.ID]

	if taskPersisted == nil {
		taskPersisted = &taskQueued

		w.Db[taskQueued.ID] = &taskQueued
	}

	var result task.DockerResult

	if task.ValidStateTransition(taskPersisted.State, taskQueued.State) {
		switch taskQueued.State {
		case task.SCHEDULED:
			result = w.StartTask(taskQueued)
		case task.COMPLETED:
			result = w.StopTask(taskQueued)
		default:
			result.Error = errors.New("we should not get there")
		}
	} else {
		result.Error = fmt.Errorf("invalid transition from %v to %v", taskPersisted.State, taskQueued.State)
	}

	return result
}

func (w *Worker) AddTask(t task.Task) {
	w.Queue.Enqueue(t)
}

func (w *Worker) StartTask(t task.Task) task.DockerResult {
	t.StartTime = time.Now().UTC()

	dockerContainer := task.NewDockerContainer(task.NewConfig(&t))

	result := dockerContainer.Run()

	if result.Error != nil {
		log.Printf("Err running task %v: %+v\n", t.ID, result.Error)

		t.State = task.FAILED

		w.Db[t.ID] = &t

		return result
	}

	t.ContainerID = result.ContainerID
	t.State = task.RUNNING

	w.Db[t.ID] = &t

	return result
}

func (w *Worker) StopTask(t task.Task) task.DockerResult {
	dockerContainer := task.NewDockerContainer(task.NewConfig(&t))

	result := dockerContainer.Stop(t.ContainerID)

	if result.Error != nil {
		log.Printf("Error stopping container %v: %+v\n", t.ContainerID, result.Error)
	}

	t.FinishTime = time.Now().UTC()
	t.State = task.COMPLETED

	w.Db[t.ID] = &t

	log.Printf("Stopped and removed container %v for task %+v\n")

	return result
}

func (w *Worker) CollectStats() {
	for {
		log.Println("Collecting stats")

		w.Stats = GetStats()
		w.Stats.TaskCount = w.TaskCount

		time.Sleep(time.Second * 10)
	}
}

func (w *Worker) GetTasks() []*task.Task {
	var tasks []*task.Task

	for _, t := range w.Db {
		tasks = append(tasks, t)
	}

	return tasks
}

func (w *Worker) RunTasks() {
	for {
		if w.Queue.Len() != 0 {
			if result := w.runTask(); result.Error != nil {
				log.Printf("Error running task: %v\n", result.Error)
			}
		} else {
			log.Printf("No tasks to process currently.\n")
		}

		log.Println("Sleeping for 5 seconds.")

		time.Sleep(5 * time.Second)
	}
}

func (w *Worker) InspectTask(t task.Task) task.DockerInspectResponse {
	config := task.NewConfig(&t)

	dockerContainer := task.NewDockerContainer(config)

	return dockerContainer.Inspect(t.ContainerID)
}

func (w *Worker) UpdateTasks() {
	for {
		log.Println("Checking status of tasks")

		w.updateTasks()

		log.Println("Task updates completed")
		log.Println("Sleeping for 15 seconds")

		time.Sleep(15 * time.Second)
	}
}

func (w *Worker) updateTasks() {
	for id, t := range w.Db {
		if t.State != task.RUNNING {
			continue
		}
		resp := w.InspectTask(*t)

		if resp.Error != nil {
			fmt.Printf("ERROR: %v\n", resp.Error)
		}

		if resp.Container == nil {
			log.Printf("No container for running task %s\n", id)
			w.Db[id].State = task.FAILED
		}

		if resp.Container.State.Status == "exited" {
			log.Printf(
				"Container for task %s in non-running state %s",
				id, resp.Container.State.Status,
			)
			w.Db[id].State = task.FAILED
		}
		w.Db[id].HostPorts = resp.Container.NetworkSettings.NetworkSettingsBase.Ports
	}
}
