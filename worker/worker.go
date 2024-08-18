package worker

import (
	"errors"
	"fmt"
	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
	"log"
	"orchestrator/task"
	"time"
)

type Worker struct {
	Name      string
	Queue     queue.Queue
	Db        map[uuid.UUID]*task.Task
	TaskCount int
}

func (w *Worker) RunTask() task.DockerResult {
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
			result = w.StartTask(taskQueued)
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
	fmt.Println("I will collect stats")
}

func (w *Worker) GetTasks() []*task.Task {
	var tasks []*task.Task

	for _, t := range w.Db {
		tasks = append(tasks, t)
	}

	return tasks
}
