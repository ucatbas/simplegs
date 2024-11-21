package simplegs

import (
	"log"
	"sync"

	"github.com/ucatbas/simplegs/models"
)

type Worker struct {
	Name         string
	storage      Storage
	requests     chan *models.Request
	free         bool
	done         chan *Worker
	closeChannel chan chan bool
	quit         chan struct{} // New quit channel for forced termination
	bufferSize   int
}

func NewWorker(storage Storage, name string, done chan *Worker, bufferSize int) *Worker {
	// Create a new worker instance
	worker := &Worker{
		Name:         name,
		storage:      storage,
		requests:     make(chan *models.Request, bufferSize), // Channel to receive job requests
		free:         true,
		done:         done,
		closeChannel: make(chan chan bool), // Channel to handle worker shutdown signals
		quit:         make(chan struct{}),  // Channel for forced termination
		bufferSize:   bufferSize,
	}

	// Start the worker's processing goroutine
	return worker
}

// Run starts the worker's goroutine to process jobs concurrently.
func (w *Worker) Run() {
	var wg sync.WaitGroup

	go func() {
		for {
			select {
			case req := <-w.requests:
				wg.Add(1)
				go func(request *models.Request) {
					defer wg.Done()
					w.process(request) // Process the request
				}(req)

			case cb := <-w.closeChannel:
				log.Printf("Worker [%s] shutting down\n", w.Name)

				// Wait for all ongoing tasks to finish
				wg.Wait()
				cb <- true
				return

			case <-w.quit: // Forced termination
				log.Printf("Worker [%s] forced to shut down\n", w.Name)
				return
			}
		}
	}()
}

// Stop signals the worker to stop processing jobs gracefully or forcefully.
func (w *Worker) Stop(force bool) {
	log.Printf("Stopping worker [%s]...\n", w.Name)

	if force {
		// Send a quit signal to terminate the worker immediately
		close(w.quit)
		log.Printf("Worker [%s] forced to stop.\n", w.Name)
	} else {
		// Graceful shutdown
		cb := make(chan bool)
		w.closeChannel <- cb // Send the stop signal
		<-cb                 // Wait for confirmation
		log.Printf("Worker [%s] has stopped gracefully.\n", w.Name)
	}
}

// DoWork assigns a job to the worker's requests channel.
func (w *Worker) DoWork(req *models.Request) {
	w.requests <- req
}

// Close signals the worker to stop processing jobs.
func (w *Worker) Close(cb chan bool) {
	w.closeChannel <- cb
}

func (w *Worker) process(req *models.Request) {
	w.free = false                   // Increment pending tasks count
	defer func() { w.free = true }() // Decrement after processing

	// Check if the job is canceled
	if req.IsCanceled() {
		log.Printf("Worker [%s] skipping canceled job [%s]\n", w.Name, req.ID)
		return
	}

	// Execute the task's callback function
	taskResponse := req.Task.Callback()

	// Check again if the job was canceled mid-execution
	if req.IsCanceled() {
		log.Printf("Worker [%s] skipping canceled job [%s] mid-execution\n", w.Name, req.ID)
		return
	}

	// Handle the callback result
	if taskResponse.Error != nil {
		log.Printf("Worker [%s] encountered an error while processing job [%s]: %v", w.Name, req.ID, taskResponse.Error)
		return
	}

	// Mark the request as complete
	req.MarkComplete(taskResponse)

	// Save the request state to storage
	if err := w.storage.Save(req); err != nil {
		log.Printf("Failed to update job state for job [%s]: %v", req.ID, err)
	}

}
