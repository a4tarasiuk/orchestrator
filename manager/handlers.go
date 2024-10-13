package manager

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"orchestrator/task"
)

func (a *API) StartTaskHandler(w http.ResponseWriter, r *http.Request) {
	d := json.NewDecoder(r.Body)
	d.DisallowUnknownFields()
	te := task.TaskEvent{}
	err := d.Decode(&te)
	if err != nil {
		msg := fmt.Sprintf("Error unmarshalling body: %v\n", err)
		log.Printf(msg)
		w.WriteHeader(400)
		e := ErrorResponse{
			HTTPStatusCode: 400,
			Message:        msg,
		}
		json.NewEncoder(w).Encode(e)
		return
	}

	a.Manager.AddTask(te)
	log.Printf("Added task %v\n", te.Task.ID)
	w.WriteHeader(201)

	json.NewEncoder(w).Encode(te.Task)
}

func (a *API) GetTasksHandler(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)

	json.NewEncoder(w).Encode(a.Manager.GetTasks())
}

func (a *API) StopTaskHandler(w http.ResponseWriter, r *http.Request) {
	taskID := chi.URLParam(r, "taskID")
	if taskID == "" {
		log.Printf("No taskID passed in request.\n")
		w.WriteHeader(http.StatusBadRequest)
	}

	tID, _ := uuid.Parse(taskID)
	result, err := a.Manager.TaskStore.Get(tID.String())
	if err != nil {
		log.Printf("No task with ID %v found", tID)
		w.WriteHeader(http.StatusNotFound)
	}

	taskToStop, ok := result.(*task.Task)
	if !ok {
		log.Printf("unable to convert task to task.Task type")
		return
	}

	te := task.TaskEvent{
		ID:        uuid.New(),
		State:     task.COMPLETED,
		Timestamp: time.Now(),
	}

	taskCopy := *taskToStop
	taskCopy.State = task.COMPLETED

	te.Task = taskCopy
	a.Manager.AddTask(te)

	log.Printf("Added task event %v to stop task %v\n", te.ID, taskToStop.ID)

	w.WriteHeader(http.StatusNoContent)
}

func (a *API) Start() {
	a.initRouter()
	http.ListenAndServe(fmt.Sprintf("%s:%d", a.Address, a.Port), a.Router)
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
}
