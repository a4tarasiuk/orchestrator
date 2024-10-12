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

	fmt.Println("Starting Cube workers")

	startWorker(workerHost, workerPort)
	startWorker(workerHost, workerPort+1)
	startWorker(workerHost, workerPort+2)

	time.Sleep(time.Second * 3)

	workers := []string{
		fmt.Sprintf("%s:%d", workerHost, workerPort),
		fmt.Sprintf("%s:%d", workerHost, workerPort+1),
		fmt.Sprintf("%s:%d", workerHost, workerPort+2),
	}

	m := manager.New(workers, "epvm")

	managerAPI := manager.API{Address: managerHost, Port: managerPort, Manager: m}

	go m.ProcessTasks()
	go m.UpdateTasks()
	go m.DoHealthChecks()

	managerAPI.Start()
}

func startWorker(host string, port int) worker.Worker {
	w := worker.Worker{
		Queue: *queue.New(),
		Db:    make(map[uuid.UUID]*task.Task),
		Stats: nil,
	}

	go w.RunTasks()
	go w.CollectStats()
	go w.UpdateTasks()

	workerAPI := worker.API{Address: host, Port: port, Worker: &w}
	go workerAPI.Start()

	return w
}
