package main

import (
	"fmt"
	"log"
	"time"

	"github.com/golang-collections/collections/queue"
	"orchestrator/manager"
	"orchestrator/store"
	"orchestrator/worker"
)

func main() {
	managerHost := "localhost"
	managerPort := 8000

	workerHost := "localhost"
	workerPort := 8001

	fmt.Println("Starting Cube workers")

	startWorker(workerHost, workerPort)
	// startWorker(workerHost, workerPort+1)
	// startWorker(workerHost, workerPort+2)
	//
	time.Sleep(time.Second * 3)

	workers := []string{
		fmt.Sprintf("%s:%d", workerHost, workerPort),
		// fmt.Sprintf("%s:%d", workerHost, workerPort+1),
		// fmt.Sprintf("%s:%d", workerHost, workerPort+2),
	}

	var err error
	var managerTaskStore *store.BoltDBTaskStore
	var managerTaskEventStore *store.BoltDBTaskEventStore

	managerTaskStore, err = store.NewBoltDBTaskStore("tasks.db", 0600, "tasks")
	managerTaskEventStore, err = store.NewBoltDbTaskEventStore("events.db", 0600, "events")

	if err != nil {
		log.Fatal("Error during setup databases for Store", err)
	}

	workerClient := manager.WorkerHTTPClient{}

	m := manager.New(workers, "epvm", managerTaskStore, managerTaskEventStore, workerClient)

	managerAPI := manager.API{Address: managerHost, Port: managerPort, Manager: m}

	go m.ProcessTasks()
	go m.UpdateTasks()
	go m.DoHealthChecks()

	managerAPI.Start()
}

func startWorker(host string, port int) worker.Worker {
	filename := fmt.Sprintf("worker_%d_tasks.db", port)

	taskStore, err := store.NewBoltDBTaskStore(filename, 0600, "tasks")

	if err != nil {
		log.Fatalf("Error during setuping db for worker %d", port)
	}

	w := worker.Worker{
		Queue:     *queue.New(),
		Stats:     nil,
		TaskStore: taskStore,
	}

	go w.RunTasks()
	go w.CollectStats()
	go w.UpdateTasks()

	workerAPI := worker.API{Address: host, Port: port, Worker: &w}
	go workerAPI.Start()

	return w
}
