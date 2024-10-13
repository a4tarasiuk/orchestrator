package worker

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"orchestrator/task"
)

type ErrResponse struct {
	HTTPStatusCode int
	Message        string
}

func (a *API) StartTaskHandler(w http.ResponseWriter, r *http.Request) {
	decoder := json.NewDecoder(r.Body)
	decoder.DisallowUnknownFields()

	taskEvent := task.TaskEvent{}

	if err := decoder.Decode(&taskEvent); err != nil {
		msg := fmt.Sprintf("Error unmarshalling body: %v\n", err)
		log.Printf(msg)

		w.WriteHeader(400)

		errorResponse := ErrResponse{HTTPStatusCode: 400, Message: msg}

		json.NewEncoder(w).Encode(errorResponse)

		return
	}

	a.Worker.AddTask(taskEvent.Task)

	log.Printf("Worker Added task %v\n", taskEvent.Task.ID)

	w.WriteHeader(201)

	json.NewEncoder(w).Encode(taskEvent.Task)
}

func (a *API) GetTasksHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(200)

	json.NewEncoder(w).Encode(a.Worker.GetTasks())
}

func (a *API) StopTaskHandler(w http.ResponseWriter, r *http.Request) {
	taskIDParam := chi.URLParam(r, "taskID")

	if taskIDParam == "" {
		log.Printf("No taskID passed in request.\n")
		w.WriteHeader(400)
	}

	taskID, _ := uuid.Parse(taskIDParam)

	taskToStop, err := a.Worker.TaskStore.Get(taskID.String())
	if err != nil {
		log.Printf("No task with ID %v found", taskID)
		w.WriteHeader(404)
	}

	taskCopy := *taskToStop.(*task.Task)
	taskCopy.State = task.COMPLETED

	a.Worker.AddTask(taskCopy)

	log.Printf("Added task %v to stop container %v\n", taskCopy.ID, taskCopy.ContainerID)

	w.WriteHeader(204)
}

func (a *API) GetStatsHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	json.NewEncoder(w).Encode(a.Worker.Stats)
}

func (a *API) initRouter() {
	a.Router = chi.NewRouter()
	a.Router.Route(
		"/tasks", func(r chi.Router) {
			r.Post("/", a.StartTaskHandler)
			r.Get("/", a.GetTasksHandler)
			r.Route(
				"/{taskID}", func(r chi.Router) {
					r.Delete("/", a.StopTaskHandler)
				},
			)
		},
	)
	a.Router.Route(
		"/stats", func(r chi.Router) {
			r.Get("/", a.GetStatsHandler)
		},
	)
}

func (a *API) Start() {
	a.initRouter()

	http.ListenAndServe(fmt.Sprintf("%s:%d", a.Address, a.Port), a.Router)
}
