package worker

import (
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/golang-collections/collections/queue"
	"orchestrator/store"
	"orchestrator/task"
)

type Worker struct {
	Name string

	Queue queue.Queue

	TaskStore store.Store

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

	err := w.TaskStore.Put(taskQueued.ID.String(), &taskQueued)
	if err != nil {
		msg := fmt.Errorf("error storing task %s: %v", taskQueued.ID.String(), err)
		log.Println(msg)
		return task.DockerResult{Error: msg}
	}

	queuedTask, err := w.TaskStore.Get(taskQueued.ID.String())
	if err != nil {
		msg := fmt.Errorf("error getting task %s from database: %v", taskQueued.ID.String(), err)
		log.Println(msg)
		return task.DockerResult{Error: msg}
	}
	taskPersisted := *queuedTask.(*task.Task)

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

		w.TaskStore.Put(t.ID.String(), &t)

		return result
	}

	t.ContainerID = result.ContainerID
	t.State = task.RUNNING

	w.TaskStore.Put(t.ID.String(), &t)

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

	w.TaskStore.Put(t.ID.String(), &t)

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
	taskList, err := w.TaskStore.List()

	if err != nil {
		log.Printf("error getting list of tasks: %v\n", err)
		return nil
	}

	return taskList.([]*task.Task)
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
	tasks, err := w.TaskStore.List()
	if err != nil {
		log.Printf("error getting list of tasks: %v\n", err)
		return
	}

	for _, t := range tasks.([]*task.Task) {
		if t.State == task.RUNNING {
			resp := w.InspectTask(*t)
			if resp.Error != nil {
				fmt.Printf("ERROR: %v\n", resp.Error)
			}

			if resp.Container == nil {
				log.Printf("No container for running task %s\n", t.ID)
				t.State = task.FAILED
				w.TaskStore.Put(t.ID.String(), t)
			}

			if resp.Container.State.Status == "exited" {
				log.Printf("Container for task %s in non-running state %s\n", t.ID, resp.Container.State.Status)
				t.State = task.FAILED
				w.TaskStore.Put(t.ID.String(), t)
			}

			// task is running, update exposed ports
			t.HostPorts = resp.Container.NetworkSettings.NetworkSettingsBase.Ports
			w.TaskStore.Put(t.ID.String(), t)
		}
	}
}
