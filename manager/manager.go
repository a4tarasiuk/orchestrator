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
	"orchestrator/task"
	"orchestrator/worker"
)

type Manager struct {
	PendingEvents queue.Queue

	TaskDb map[uuid.UUID]*task.Task

	EventDb map[uuid.UUID]*task.TaskEvent

	Workers []string

	WorkerTaskMap map[string][]uuid.UUID

	TaskWorkerMap map[uuid.UUID]string

	lastWorkerIdx int
}

func New(workers []string) *Manager {
	taskDb := make(map[uuid.UUID]*task.Task)
	eventDb := make(map[uuid.UUID]*task.TaskEvent)

	workerTaskMap := make(map[string][]uuid.UUID)

	for w := range workers {
		workerTaskMap[workers[w]] = []uuid.UUID{}
	}

	taskWorkerMap := make(map[uuid.UUID]string)

	return &Manager{
		PendingEvents: *queue.New(),
		TaskDb:        taskDb,
		EventDb:       eventDb,
		Workers:       workers,
		WorkerTaskMap: workerTaskMap,
		TaskWorkerMap: taskWorkerMap,
		lastWorkerIdx: 0,
	}
}

func (m *Manager) AddTask(taskEvent task.TaskEvent) {
	m.PendingEvents.Enqueue(taskEvent)
}

func (m *Manager) SelectWorker() string {
	var selectedWorkerIdx int

	if m.lastWorkerIdx+1 < len(m.Workers) {
		selectedWorkerIdx = m.lastWorkerIdx + 1
		m.lastWorkerIdx++
	} else {
		selectedWorkerIdx = 0
		m.lastWorkerIdx = 0
	}

	return m.Workers[selectedWorkerIdx]
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

			_, ok := m.TaskDb[taskObj.ID]

			if !ok {
				log.Printf("Task with ID %s not found\n", taskObj.ID)
				return
			}

			dbTaskObj := m.TaskDb[taskObj.ID]

			if dbTaskObj.State != taskObj.State {
				dbTaskObj.State = taskObj.State
			}

			dbTaskObj.StartTime = taskObj.StartTime
			dbTaskObj.FinishTime = taskObj.FinishTime

			dbTaskObj.ContainerID = taskObj.ContainerID

			// m.TaskDb[taskObj.ID] = dbTaskObj
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

	_worker := m.SelectWorker()

	event := m.PendingEvents.Dequeue()

	taskEvent := event.(task.TaskEvent)

	taskObj := taskEvent.Task

	log.Printf("Pulled %v off pending queue\n", taskObj)

	m.EventDb[taskEvent.ID] = &taskEvent

	m.WorkerTaskMap[_worker] = append(m.WorkerTaskMap[_worker], taskEvent.Task.ID)

	m.TaskWorkerMap[taskObj.ID] = _worker

	taskObj.State = task.SCHEDULED
	m.TaskDb[taskObj.ID] = &taskObj

	data, err := json.Marshal(taskEvent)

	if err != nil {
		log.Printf("Unnable to marshal task object: %v\n", taskObj)
	}

	url := fmt.Sprintf("http://%s/tasks", _worker)

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
	var tasks []*task.Task

	for _, t := range m.TaskDb {
		tasks = append(tasks, t)
	}

	return tasks
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
	m.TaskDb[t.ID] = t

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

func getHostPort(ports nat.PortMap) *string {
	for k, _ := range ports {
		return &ports[k][0].HostPort
	}
	return nil
}
