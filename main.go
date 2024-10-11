package main

import (
	"fmt"
	"log"
	"time"

	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
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

	api.Start()
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
