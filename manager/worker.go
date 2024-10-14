package manager

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/google/uuid"
	"orchestrator/task"
)

type (
	WorkerClient interface {
		StartTask(workerHostPort string, taskEvent task.TaskEvent) error

		StopTask(workerHostPort string, taskID uuid.UUID) error

		GetTasks(workerHostPort string) ([]*task.Task, error)

		GetTaskHealth(healthRequest WorkerTaskHealthRequest) (*WorkerTaskHealthResult, error)
	}

	WorkerTaskHealthRequest struct {
		TaskID uuid.UUID

		Host string
		Port string
		Path string
	}

	WorkerTaskHealthResult struct {
		TaskID uuid.UUID

		IsHealthy bool

		OccurredAt time.Time
	}
)

type WorkerHTTPClient struct {
}

func (wc WorkerHTTPClient) StartTask(workerHostPort string, taskEvent task.TaskEvent) error {
	data, err_ := json.Marshal(taskEvent)

	if err_ != nil {
		msg := fmt.Sprintf("Unnable to marshal task event: %v\n", taskEvent)

		return errors.New(msg)
	}

	url := fmt.Sprintf("http://%s/tasks", workerHostPort)

	response, err := http.Post(url, "application/json", bytes.NewBuffer(data))

	responseBody := json.NewDecoder(response.Body)

	if response.StatusCode != http.StatusCreated {
		errResponse := ErrorResponse{}

		err = responseBody.Decode(&errResponse)

		var msg string

		if err != nil {
			msg = fmt.Sprintf("Error decoding response: %s\n", err.Error())
		} else {
			msg = fmt.Sprintf("Response error (%d): %s", errResponse.HTTPStatusCode, errResponse.Message)
		}

		return errors.New(msg)
	}

	return nil
}

func (wc WorkerHTTPClient) StopTask(workerHostPort string, taskID uuid.UUID) error {
	client := &http.Client{}

	url := fmt.Sprintf("http://%s/tasks/%s", workerHostPort, taskID)

	request, err := http.NewRequest("DELETE", url, nil)

	if err != nil {
		log.Printf("error creating request to delete task %s: %v\n", taskID, err)
		return err
	}

	response, err := client.Do(request)
	if err != nil {
		log.Printf("error connecting to worker at %s: %v\n", url, err)
		return err
	}

	if response.StatusCode != http.StatusNoContent {
		log.Printf("error sending request: %v\n", err)
		return err
	}

	return nil
}

func (wc WorkerHTTPClient) GetTasks(workerHostPort string) ([]*task.Task, error) {
	url := fmt.Sprintf("http://%s/tasks", workerHostPort)

	response, err := http.Get(url)

	if err != nil {
		msg := fmt.Sprintf("Error connecting to %v: %v\n", workerHostPort, err)

		return nil, errors.New(msg)
	}

	if response.StatusCode != http.StatusOK {
		msg := fmt.Sprintf("Error sending request: %v\n", err)

		return nil, errors.New(msg)
	}

	decoder := json.NewDecoder(response.Body)

	var tasks []*task.Task

	err = decoder.Decode(&tasks)

	if err != nil {
		msg := fmt.Sprintf("Error unmarshalling tasks: %s\n", err.Error())

		return nil, errors.New(msg)
	}

	return tasks, nil
}

func (wc WorkerHTTPClient) GetTaskHealth(healthRequest WorkerTaskHealthRequest) (*WorkerTaskHealthResult, error) {

	url := fmt.Sprintf("http://%s:%s%s", healthRequest.Host, healthRequest.Port, healthRequest.Path)

	taskID := healthRequest.TaskID

	response, err := http.Get(url)

	if err != nil {
		msg := fmt.Sprintf("Error connecting to health check %s", url)
		log.Println(msg)

		return nil, errors.New(msg)
	}

	healthResult := &WorkerTaskHealthResult{
		TaskID:     taskID,
		OccurredAt: time.Now(),
		IsHealthy:  response.StatusCode == http.StatusOK,
	}

	log.Printf("Task %s health check response: %v\n", taskID, response.StatusCode)

	return healthResult, nil
}
