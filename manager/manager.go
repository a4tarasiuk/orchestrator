package manager

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"net/http"

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

func (m *Manager) UpdateTasks() {
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
