package main

import (
	"fmt"
	"log"
	"time"

	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
	"orchestrator/manager"
	"orchestrator/task"
	"orchestrator/worker"
)

func main() {
	host := "localhost"
	port := 8000

	fmt.Println("Starting Cube worker")

	w := worker.Worker{
		Queue: *queue.New(),
		Db:    make(map[uuid.UUID]*task.Task),
	}

	api := worker.API{Address: host, Port: port, Worker: &w}

	go runTasks(&w)

	go w.CollectStats()

	go api.Start()

	workers := []string{fmt.Sprintf("%s:%d", host, port)}

	m := manager.New(workers)

	time.Sleep(time.Second * 5)

	for i := 0; i < 2; i++ {
		t := task.Task{
			ID:    uuid.New(),
			Name:  fmt.Sprintf("test-container-%d", i),
			State: task.SCHEDULED,
			Image: "strm/helloworld-http",
		}

		te := task.TaskEvent{
			ID:    uuid.New(),
			State: task.RUNNING,
			Task:  t,
		}

		m.AddTask(te)

		m.SendWork()
	}

	go func() {
		for {
			fmt.Printf("[Manager] Updating tasks from %d workers\n", len(m.Workers))

			m.UpdateTasks()

			time.Sleep(15 * time.Second)
		}
	}()

	for {
		for _, t := range m.TaskDb {
			fmt.Printf("[Manager] Task: id: %s, state: %d\n", t.ID, t.State)

			time.Sleep(15 * time.Second)
		}
	}
}

func runTasks(w *worker.Worker) {
	for {
		if w.Queue.Len() != 0 {
			if result := w.RunTask(); result.Error != nil {
				log.Printf("Error running task: %v\n", result.Error)
			}
		} else {
			log.Printf("No tasks to process currently.\n")
		}

		log.Println("Sleeping for 5 seconds.")

		time.Sleep(5 * time.Second)
	}
}
