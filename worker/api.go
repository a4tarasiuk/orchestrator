package worker

import "github.com/go-chi/chi/v5"

type API struct {
	Address string
	Port    int
	Worker  *Worker
	Router  *chi.Mux
}

type ErrorResponse struct {
	HTTPStatusCode int
	Message        string
}
