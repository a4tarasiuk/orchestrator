package main

import (
	"fmt"
	"time"

	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
	"orchestrator/manager"
	"orchestrator/task"
	"orchestrator/worker"
)

func main() {
	managerHost := "localhost"
	managerPort := 8000

	workerHost := "localhost"
	workerPort := 8001

	fmt.Println("Starting Cube worker")

	w := worker.Worker{
		Queue: *queue.New(),
		Db:    make(map[uuid.UUID]*task.Task),
		Stats: nil,
	}

	workerAPI := worker.API{Address: workerHost, Port: workerPort, Worker: &w}

	go w.RunTasks()
	go w.CollectStats()
	go workerAPI.Start()

	time.Sleep(time.Second * 3)

	workers := []string{fmt.Sprintf("%s:%d", workerHost, workerPort)}

	m := manager.New(workers)

	managerAPI := manager.API{Address: managerHost, Port: managerPort, Manager: m}

	go m.ProcessTasks()
	go m.UpdateTasks()

	managerAPI.Start()
}
