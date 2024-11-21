package simplegs

import (
	"container/heap"
	"fmt"
	"log"
	"sync"
	"time"

	"github.com/ucatbas/simplegs/models"
)

type Scheduler struct {
	jobQueue           JobQueue
	jobQueueSet        map[string]bool
	jobMutex           sync.Mutex
	done               chan *Worker
	pool               []*Worker
	storage            Storage
	retryDelayInterval time.Duration
}

func NewScheduler(storage Storage, retryDelayInterval time.Duration, maxJobs int) *Scheduler {
	if storage == nil {
		storage = NewInMemoryStorage() // Use in-memory storage as fallback
	}

	scheduler := &Scheduler{
		jobQueue:           make(JobQueue, 0, maxJobs),
		jobQueueSet:        map[string]bool{},
		jobMutex:           sync.Mutex{},
		done:               make(chan *Worker),
		storage:            storage,
		retryDelayInterval: retryDelayInterval,
	}

	return scheduler
}

func (s *Scheduler) Start(workerCount int, taskPerWorker int) {
	heap.Init(&s.jobQueue)

	// Initialize workers
	for i := 0; i < workerCount; i++ {
		worker := NewWorker(s.storage, fmt.Sprintf("Worker-%d", i), s.done, taskPerWorker)
		s.pool = append(s.pool, worker)
		worker.Run()
	}

	s.startJobLoader()
	s.startDispatch()
}

func (s *Scheduler) Stop() {
	log.Println("Stopping scheduler...")

	// Step 1: Stop workers
	for _, worker := range s.pool {
		worker.Stop(true) // Gracefully stop each worker
	}

	// Step 2: Close the dispatcher and loader
	close(s.done) // Closing `done` channel to exit `startDispatch`
	log.Println("Scheduler stopped.")
}

func (s *Scheduler) PostJob(job *models.Request) error {
	s.jobMutex.Lock()
	defer s.jobMutex.Unlock()

	// Ensure job is not already in the queue
	for _, existingJob := range s.jobQueue {
		if existingJob.ID == job.ID {
			return fmt.Errorf("job with ID %s already exists", job.ID)
		}
	}

	// Validate the job
	err := job.Validate()
	if err != nil {
		return err
	}

	// Save the job to storage
	if err := s.storage.Save(job); err != nil {
		return err
	}

	// Trigger dispatch to process the job immediately if there are available workers
	select {
	case s.done <- nil: // Notify dispatcher there are jobs to process
	default:
	}

	return nil
}

func (s *Scheduler) CancelJob(jobID string) error {
	s.jobMutex.Lock()
	defer s.jobMutex.Unlock()

	var canceledJob *models.Request

	// Check the job queue first
	_, exists := s.jobQueueSet[jobID]
	if exists {
		// Find the job in the queue
		for i, job := range s.jobQueue {
			if job.ID == jobID {
				// Remove job from the queue
				canceledJob = job
				// Swap the job with the last element
				lastIndex := len(s.jobQueue) - 1
				s.jobQueue[i], s.jobQueue[lastIndex] = s.jobQueue[lastIndex], s.jobQueue[i]

				// Remove the last element (which is now the job to be removed)
				s.jobQueue = s.jobQueue[:lastIndex]

				// Restore heap property
				heap.Fix(&s.jobQueue, i)

				// Delete the job from the set
				delete(s.jobQueueSet, jobID)
				break
			}
		}
	} else {
		canceledJob, _ = s.storage.GetJobByID(jobID)
	}

	// Mark the job as canceled
	canceledJob.MarkCanceled()

	// Update job state in the database
	if err := s.storage.Save(canceledJob); err != nil {
		return fmt.Errorf("failed to update canceled job in storage: %v", err)
	}

	return nil
}

func (s *Scheduler) TriggerJob(jobID string) error {
	s.jobMutex.Lock()
	defer s.jobMutex.Unlock()

	var triggeredJob *models.Request

	_, exists := s.jobQueueSet[jobID]
	if exists {
		// Find the job in the queue
		for i, job := range s.jobQueue {
			if job.ID == jobID {
				// Remove job from the queue
				triggeredJob = job
				// Swap the job with the last element
				lastIndex := len(s.jobQueue) - 1
				s.jobQueue[i], s.jobQueue[lastIndex] = s.jobQueue[lastIndex], s.jobQueue[i]

				// Remove the last element (which is now the job to be removed)
				s.jobQueue = s.jobQueue[:lastIndex]

				// Restore heap property
				heap.Fix(&s.jobQueue, i)

				// Delete the job from the set
				delete(s.jobQueueSet, jobID)
				break
			}
		}
	} else {
		triggeredJob, _ = s.storage.GetJobByID(jobID)
	}

	// Execute the job immediately
	go s.dispatch(triggeredJob)

	return nil
}

func (s *Scheduler) startJobLoader() {
	go func() {
		for {
			time.Sleep(s.retryDelayInterval) // Sleep before checking for new jobs

			// Load jobs that need to be executed from the database
			jobs, err := s.storage.GetScheduledJobs()

			if err != nil {
				log.Printf("Failed to load scheduled jobs: %v", err)
				continue
			} else if len(jobs) == 0 {
				continue
			}

			// Add jobs to the job queue
			s.jobMutex.Lock()
			for _, job := range jobs {
				_, exists := s.jobQueueSet[job.ID]
				if !exists {
					heap.Push(&s.jobQueue, job)
					s.jobQueueSet[job.ID] = true
				}
			}
			s.jobMutex.Unlock()

			// Notify dispatcher if new jobs are available
			select {
			case s.done <- nil: // Wake up the dispatcher
			default:
			}
		}
	}()
}
func (s *Scheduler) startDispatch() {
	// This should run in a separate goroutine
	go func() {
		for range s.done { // Wait for notification when there are jobs to process
			s.jobMutex.Lock()

			// If there are jobs in the queue, attempt to dispatch them
			if len(s.jobQueue) > 0 {
				// Get the job with the earliest execution time
				job := heap.Pop(&s.jobQueue).(*models.Request)
				delete(s.jobQueueSet, job.ID)

				s.jobMutex.Unlock()

				// Find an available worker
				workerFound := false
				for _, worker := range s.pool {
					if len(worker.requests) < worker.bufferSize { // Assuming `pending` tracks the number of jobs the worker is handling
						worker.DoWork(job)
						workerFound = true
						break
					}
				}

				// If no worker is available, re-add the job to the queue
				if !workerFound {
					fmt.Printf("Worker not found, length of pool is: %d \n", len(s.pool))
					s.jobMutex.Lock() // Lock again before modifying the queue
					heap.Push(&s.jobQueue, job)
					s.jobQueueSet[job.ID] = true
					s.jobMutex.Unlock()
				}
			} else {
				s.jobMutex.Unlock()
			}
		}
	}()
}

func (s *Scheduler) dispatch(r *models.Request) {
	go func() {
		for {
			// Find an available worker
			for _, worker := range s.pool {
				if worker.free { // Assuming `pending` tracks the number of jobs the worker is handling
					worker.DoWork(r)
					break
				}
			}
			time.Sleep(s.retryDelayInterval)
		}
	}()
}
