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

	return &Manager{
		PendingEvents:  *queue.New(),
		TaskStore:      taskStore,
		TaskEventStore: eventStore,
		Workers:        workers,
		WorkerTaskMap:  workerTaskMap,
		TaskWorkerMap:  taskWorkerMap,
		lastWorkerIdx:  0,
		WorkerNodes:    nodes,
		Scheduler:      newScheduler,
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
		url := fmt.Sprintf("http://%s/tasks", w)
		resp, err := http.Get(url)
		if err != nil {
			log.Printf("Error connecting to %v: %v\n", w, err)
		}
		if resp.StatusCode != http.StatusOK {
			log.Printf("Error sending request: %v\n", err)
		}
		d := json.NewDecoder(resp.Body)
		var tasks []*task.Task
		err = d.Decode(&tasks)
		if err != nil {
			log.Printf("Error unmarshalling tasks: %s\n", err.Error())
		}

		for _, taskObj := range tasks {
			log.Printf("Attempting to update task %v\n", taskObj.ID)

			result, err := m.TaskStore.Get(taskObj.ID.String())

			if err != nil {
				log.Printf("[manager] %s\n", err)
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
		result, err := m.TaskStore.Get(taskEvent.Task.ID.String())
		if err != nil {
			log.Printf("unable to schedule task: %s", err)
			return
		}
		persistedTask, ok := result.(*task.Task)
		if !ok {
			log.Printf("unable to convert task to task.Task type")
			return
		}

		fmt.Println(
			taskEvent.State,
			persistedTask.State,
			task.ValidStateTransition(persistedTask.State, taskEvent.State),
		)

		if taskEvent.State == task.COMPLETED && task.ValidStateTransition(persistedTask.State, taskEvent.State) {
			m.stopTask(taskWorker, taskEvent.Task.ID.String())
			return
		}

		log.Printf(
			"invalid request: existing task %s is in state %v and cannot transition to the completed state\n",
			persistedTask.ID.String(),
			persistedTask.State,
		)
		return
	}

	taskObj := taskEvent.Task

	_worker, err := m.SelectWorker(taskObj)
	if err != nil {
		log.Printf("error selecting worker for task %s: %v\n", taskObj.ID, err)
	}

	m.WorkerTaskMap[_worker.Name] = append(m.WorkerTaskMap[_worker.Name], taskEvent.Task.ID)

	m.TaskWorkerMap[taskObj.ID] = _worker.Name

	taskObj.State = task.SCHEDULED
	m.TaskStore.Put(taskObj.ID.String(), &taskObj)

	data, err_ := json.Marshal(taskEvent)

	if err_ != nil {
		log.Printf("Unnable to marshal task object: %v\n", taskObj)
	}

	url := fmt.Sprintf("http://%s/tasks", _worker.Name)

	response, _err := http.Post(url, "application/json", bytes.NewBuffer(data))

	if _err != nil {
		log.Printf("Error connecting to %v: %v\n", _worker, _err)

		m.PendingEvents.Enqueue(taskEvent)

		return
	}

	responseBody := json.NewDecoder(response.Body)

	if response.StatusCode != http.StatusCreated {
		errResponse := worker.ErrorResponse{}
		err = responseBody.Decode(&errResponse)
		if err != nil {
			fmt.Printf("Error decoding response: %s\n", err.Error())
			return
		}
		log.Printf("Response error (%d): %s", errResponse.HTTPStatusCode, errResponse.Message)
		return
	}

	taskObj = task.Task{}
	err = responseBody.Decode(&taskObj)
	if err != nil {
		fmt.Printf("Error decoding response: %s\n", err.Error())
		return
	}

	log.Printf("%#v\n", taskObj)
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
	log.Printf("Calling health check for task %s: %s\n", t.ID, t.HealthCheck)

	w := m.TaskWorkerMap[t.ID]
	hostPort := getHostPort(t.HostPorts)
	workerParts := strings.Split(w, ":")

	url := fmt.Sprintf("http://%s:%s%s", workerParts[0], *hostPort, t.HealthCheck)
	log.Printf("Calling health check for task %s: %s\n", t.ID, url)
	resp, err := http.Get(url)
	if err != nil {
		msg := fmt.Sprintf("Error connecting to health check %s", url)
		log.Println(msg)
		return errors.New(msg)
	}
	if resp.StatusCode != http.StatusOK {
		msg := fmt.Sprintf("Error health check for task %s did not return 200\n", t.ID)
		log.Println(msg)
		return errors.New(msg)
	}

	log.Printf("Task %s health check response: %v\n", t.ID, resp.StatusCode)
	return nil
}

func (m *Manager) stopTask(worker string, taskID string) {
	client := &http.Client{}
	url := fmt.Sprintf("http://%s/tasks/%s", worker, taskID)
	req, err := http.NewRequest("DELETE", url, nil)
	if err != nil {
		log.Printf("error creating request to delete task %s: %v\n", taskID, err)
		return
	}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("error connecting to worker at %s: %v\n", url, err)
		return
	}
	if resp.StatusCode != http.StatusNoContent {
		log.Printf("Error sending request: %v\n", err)
		return
	}
	log.Printf("task %s has been scheduled to be stopped", taskID)
}

func getHostPort(ports nat.PortMap) *string {
	for k, _ := range ports {
		return &ports[k][0].HostPort
	}
	return nil
}
