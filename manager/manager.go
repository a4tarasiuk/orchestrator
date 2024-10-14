package manager

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"time"

	"github.com/docker/go-connections/nat"
	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
	"orchestrator/node"
	"orchestrator/scheduler"
	"orchestrator/store"
	"orchestrator/task"
	"orchestrator/worker"
)

type Manager struct {
	PendingEvents queue.Queue

	TaskStore store.Store

	TaskEventStore store.Store

	Workers []string

	WorkerTaskMap map[string][]uuid.UUID

	TaskWorkerMap map[uuid.UUID]string

	lastWorkerIdx int

	WorkerNodes []*node.Node

	Scheduler scheduler.Scheduler

	workerAPIClient WorkerAPIClient
}

func New(workers []string, schedulerType string, taskStore store.Store, eventStore store.Store) *Manager {
	workerTaskMap := make(map[string][]uuid.UUID)

	var nodes []*node.Node

	for w := range workers {
		workerTaskMap[workers[w]] = []uuid.UUID{}

		nodeAPI := fmt.Sprintf("http://%v", workers[w])
		newNode := node.NewNode(workers[w], nodeAPI, "worker")
		nodes = append(nodes, newNode)
	}

	taskWorkerMap := make(map[uuid.UUID]string)

	var newScheduler scheduler.Scheduler
	switch schedulerType {
	case "roundrobin":
		newScheduler = &scheduler.RoundRobin{Name: "roundrobin"}

	case "epvm":
		newScheduler = &scheduler.Epvm{Name: "epvm"}

	default:
		newScheduler = &scheduler.RoundRobin{Name: "roundrobin"}
	}

	workerAPIClient := WorkerHttpAPIClient{}

	return &Manager{
		PendingEvents:   *queue.New(),
		TaskStore:       taskStore,
		TaskEventStore:  eventStore,
		Workers:         workers,
		WorkerTaskMap:   workerTaskMap,
		TaskWorkerMap:   taskWorkerMap,
		lastWorkerIdx:   0,
		WorkerNodes:     nodes,
		Scheduler:       newScheduler,
		workerAPIClient: workerAPIClient,
	}
}

func (m *Manager) AddTask(taskEvent task.TaskEvent) {
	m.PendingEvents.Enqueue(taskEvent)
}

func (m *Manager) SelectWorker(t task.Task) (*node.Node, error) {
	candidates := m.Scheduler.SelectCandidateNodes(t, m.WorkerNodes)

	if candidates == nil {
		msg := fmt.Sprintf("No available candidates match resource request for task %v", t.ID)
		err := errors.New(msg)
		return nil, err
	}

	scores := m.Scheduler.Score(t, candidates)

	selectedNode := m.Scheduler.Pick(scores, candidates)

	return selectedNode, nil
}

func (m *Manager) updateTasks() {
	for _, w := range m.Workers {
		log.Printf("Checking worker %v for task updates", w)

		tasks, err := m.workerAPIClient.GetTasks(w)

		if err != nil {
			log.Println(err)
		}

		for _, taskObj := range tasks {
			log.Printf("Attempting to update task %v\n", taskObj.ID)

			result, _err := m.TaskStore.Get(taskObj.ID.String())

			if _err != nil {
				log.Printf("[manager] %s\n", _err)
				continue
			}

			taskPersisted, ok := result.(*task.Task)
			if !ok {
				log.Printf("cannot convert result %v to task.Task type\n", result)
				continue
			}

			if taskPersisted.State != taskObj.State {
				taskPersisted.State = taskObj.State
			}

			taskPersisted.StartTime = taskObj.StartTime
			taskPersisted.FinishTime = taskObj.FinishTime

			taskPersisted.ContainerID = taskObj.ContainerID
			taskPersisted.HostPorts = taskObj.HostPorts

			m.TaskStore.Put(taskPersisted.ID.String(), taskPersisted)
		}
	}

}

func (m *Manager) UpdateTasks() {
	for {
		log.Println("Checking for task updates from workers")

		m.updateTasks()

		log.Println("Task updates completed")

		log.Println("Sleeping for 10 seconds")

		time.Sleep(20 * time.Second)
	}
}

func (m *Manager) ProcessTasks() {
	for {
		log.Println("Processing any tasks in the queue")

		m.SendWork()

		log.Println("Sleeping for 10 seconds")

		time.Sleep(10 * time.Second)
	}
}

func (m *Manager) SendWork() {
	if m.PendingEvents.Len() == 0 {
		log.Println("No work in the queue")

		return
	}
	event := m.PendingEvents.Dequeue()

	taskEvent := event.(task.TaskEvent)

	log.Printf("Pulled %v off pending queue\n", taskEvent)

	err := m.TaskEventStore.Put(taskEvent.ID.String(), &taskEvent)
	if err != nil {
		log.Printf("error attempting to store task event %s: %s\n", taskEvent.ID.String(), err)
		return
	}

	taskWorker, ok := m.TaskWorkerMap[taskEvent.Task.ID]
	if ok {
		result, _err := m.TaskStore.Get(taskEvent.Task.ID.String())
		if _err != nil {
			log.Printf("unable to schedule task: %s", err)
			return
		}
		persistedTask, _ok := result.(*task.Task)
		if !_ok {
			log.Printf("unable to convert task to task.Task type")
			return
		}

		if taskEvent.State == task.COMPLETED && task.ValidStateTransition(persistedTask.State, taskEvent.State) {
			m.stopTask(taskWorker, taskEvent.Task.ID)
			return
		}

		log.Printf(
			"invalid request: existing task %s is in state %v and cannot transition to the completed state\n",
			persistedTask.ID.String(),
			persistedTask.State,
		)
		return
	}

	taskToProcess := taskEvent.Task

	_worker, err := m.SelectWorker(taskToProcess)
	if err != nil {
		log.Printf("error selecting worker for task %s: %v\n", taskToProcess.ID, err)
	}

	m.WorkerTaskMap[_worker.Name] = append(m.WorkerTaskMap[_worker.Name], taskEvent.Task.ID)

	m.TaskWorkerMap[taskToProcess.ID] = _worker.Name

	// TODO: Avoid duplication with Task
	taskEvent.State = task.SCHEDULED
	taskToProcess.State = task.SCHEDULED

	m.TaskStore.Put(taskToProcess.ID.String(), &taskToProcess)

	taskEvent.Task = taskToProcess

	if _err := m.workerAPIClient.StartTask(_worker.Name, taskEvent); _err != nil {
		log.Printf("Error starting task %s on worker %s\n", taskToProcess.ID, _worker.Name)

		m.PendingEvents.Enqueue(taskEvent)

		return
	}
}

func (m *Manager) GetTasks() []*task.Task {
	tasks, err := m.TaskStore.List()

	if err != nil {
		log.Printf("error getting list of tasks: %v\n", err)
		return nil
	}

	return tasks.([]*task.Task)
}

func (m *Manager) doHealthChecks() {
	for _, t := range m.GetTasks() {
		if t.State == task.RUNNING && t.RestartCount < 3 {
			err := m.checkTaskHealth(*t)
			if err != nil {
				if t.RestartCount < 3 {
					m.restartTask(t)
				}
			}
		} else if t.State == task.FAILED && t.RestartCount < 3 {
			m.restartTask(t)
		}
	}
}

func (m *Manager) restartTask(t *task.Task) {
	w := m.TaskWorkerMap[t.ID]
	t.State = task.SCHEDULED
	t.RestartCount++
	m.TaskStore.Put(t.ID.String(), t)

	te := task.TaskEvent{
		ID:        uuid.New(),
		State:     task.RUNNING,
		Timestamp: time.Now(),
		Task:      *t,
	}

	data, err := json.Marshal(te)
	if err != nil {
		log.Printf("Unable to marshal task object: %v.", t)
		return
	}
	url := fmt.Sprintf("http://%s/tasks", w)
	resp, err := http.Post(url, "application/json", bytes.NewBuffer(data))
	if err != nil {
		log.Printf("Error connecting to %v: %v", w, err)
		m.PendingEvents.Enqueue(t)
		return
	}
	d := json.NewDecoder(resp.Body)
	if resp.StatusCode != http.StatusCreated {
		e := worker.ErrResponse{}
		err := d.Decode(&e)
		if err != nil {
			fmt.Printf("Error decoding response: %s\n", err.Error())
			return
		}
		log.Printf("Response error (%d): %s", e.HTTPStatusCode, e.Message)
		return
	}
	newTask := task.Task{}
	err = d.Decode(&newTask)
	if err != nil {
		fmt.Printf("Error decoding response: %s\n", err.Error())
		return
	}
	log.Printf("%#v\n", t)
}

func (m *Manager) DoHealthChecks() {
	for {
		log.Println("Performing task health check")

		m.doHealthChecks()

		log.Println("Task health checks completed")

		log.Println("Sleeping for 60 seconds")

		time.Sleep(60 * time.Second)
	}
}

func (m *Manager) checkTaskHealth(t task.Task) error {
	workerHostPort := m.TaskWorkerMap[t.ID]

	taskDestination := WorkerTaskHealthRequest{
		TaskID: t.ID,
		Host:   strings.Split(workerHostPort, ":")[0],
		Port:   *getHostPort(t.HostPorts),
		Path:   t.HealthCheck,
	}

	log.Printf("Calling health check for task %s: %+v\n", t.ID, taskDestination)

	taskHealth, err := m.workerAPIClient.GetTaskHealth(taskDestination)

	if err != nil {
		msg := fmt.Sprintf("Error health check for task %s\n", t.ID)
		log.Print(msg)

		return errors.New(msg)
	}

	if !taskHealth.IsHealthy {
		msg := fmt.Sprintf("Task %s is not healthy\n", t.ID)
		log.Printf(msg)

		return errors.New(msg)
	}

	return nil
}

func (m *Manager) stopTask(workerHostPort string, taskID uuid.UUID) {
	err := m.workerAPIClient.StopTask(workerHostPort, taskID)

	if err != nil {
		log.Printf("error stopping task %s: %v\n", taskID, err)
	}

	log.Printf("task %s has been scheduled to be stopped", taskID)
}

func getHostPort(ports nat.PortMap) *string {
	for k, _ := range ports {
		return &ports[k][0].HostPort
	}
	return nil
}
