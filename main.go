package main

import (
	"fmt"
	"github.com/golang-collections/collections/queue"
	"github.com/google/uuid"
	"log"
	"orchestrator/task"
	"orchestrator/worker"
	"time"
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

//func main() {
//	w := worker.Worker{Queue: *queue.New(), Db: make(map[uuid.UUID]*task.Task)}
//
//	t := task.Task{
//		ID:    uuid.New(),
//		Name:  "test-container-1",
//		State: task.SCHEDULED,
//		Image: "strm/helloworld-http",
//	}
//
//	fmt.Println("starting task")
//
//	w.AddTask(t)
//	result := w.RunTask()
//
//	if result.Error != nil {
//		panic(result.Error)
//	}
//
//	t.ContainerID = result.ContainerID
//
//	fmt.Printf("task %s is running in container %s\n", t.ID, t.ContainerID)
//	fmt.Println("Sleepy time")
//
//	time.Sleep(time.Second * 30)
//
//	fmt.Println("stopping task %s\n", t.ID)
//
//	t.State = task.COMPLETED
//
//	w.AddTask(t)
//
//	result = w.RunTask()
//
//	if result.Error != nil {
//		panic(result.Error)
//	}
//}

//func main() {
//fmt.Printf("Create a test container\n")
//
//container, createResult := createContainer()
//
//if createResult.Error != nil {
//	fmt.Printf("%v", createResult.Error)
//	os.Exit(1)
//}
//
//time.Sleep(time.Second * 5)
//
//fmt.Printf("stopping container %s\n", container.ContainerID)
//
//_ = stopContainer(container)

//}

//func createContainer() (*task.DockerContainer, *task.DockerResult) {
//	cfg := task.Config{
//		Name:  "test-container-1",
//		Image: "postgres:13",
//		Env: []string{
//			"POSTGRES_USER=cube",
//			"POSTGRES_PASSWORD=secret",
//		},
//	}
//
//	dockerClient, _ := client.NewClientWithOpts(client.FromEnv)
//
//	container := task.DockerContainer{Client: dockerClient, Config: cfg}
//
//	result := container.Run()
//
//	if result.Error != nil {
//		fmt.Printf("%v\n\n", result.Error)
//		return nil, nil
//	}
//
//	fmt.Printf("Container %s is running with config %+v\n", result.ContainerID, cfg)
//
//	return &container, &result
//}
//
//func stopContainer(container *task.DockerContainer) *task.DockerResult {
//	result := container.Stop(container.ContainerID)
//
//	if result.Error != nil {
//		fmt.Printf("%v\n", result.Error)
//
//		return nil
//	}
//
//	fmt.Printf("Container %s has been stopped and removed\n", container.ContainerID)
//
//	return &result
//}
