package simplegs

import (
	"errors"
	"sort"
	"sync"
	"time"

	"github.com/ucatbas/simplegs/models"
)

type InMemoryStorage struct {
	mu   sync.RWMutex
	jobs map[string]*models.Request
}

func NewInMemoryStorage() *InMemoryStorage {
	return &InMemoryStorage{
		jobs: make(map[string]*models.Request),
	}
}

func (s *InMemoryStorage) Save(job *models.Request) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.jobs[job.ID] = job
	return nil
}

func (s *InMemoryStorage) GetJobByID(jobID string) (*models.Request, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	job, exists := s.jobs[jobID]
	if !exists {
		return nil, errors.New("job not found")
	}
	return job, nil
}

func (s *InMemoryStorage) LoadAll() ([]*models.Request, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	jobs := make([]*models.Request, 0, len(s.jobs))
	for _, job := range s.jobs {
		jobs = append(jobs, job)
	}
	return jobs, nil
}

func (s *InMemoryStorage) Delete(jobID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.jobs[jobID]; !exists {
		return errors.New("job not found")
	}
	delete(s.jobs, jobID)
	return nil
}

// GetScheduledJobs fetches jobs sorted by their scheduled time
func (s *InMemoryStorage) GetScheduledJobs() ([]*models.Request, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	// Current time for filtering jobs
	now := time.Now()

	// Collect jobs that are scheduled for now or before
	var jobList []*models.Request
	for i, job := range s.jobs {
		if job.Status == "pending" && (job.ScheduledTime.Before(now) || job.ScheduledTime.Equal(now)) {
			jobList = append(jobList, job)
			s.jobs[i].Status = "processing"
		}
	}

	// Sort the filtered jobs by scheduled time
	sort.Slice(jobList, func(i, j int) bool {
		return jobList[i].ScheduledTime.Before(jobList[j].ScheduledTime)
	})

	return jobList, nil
}

// Clear removes all jobs from the storage, effectively invalidating all data
func (s *InMemoryStorage) Clear() {
	s.mu.Lock()
	defer s.mu.Unlock()

	// Clear the jobs map
	s.jobs = make(map[string]*models.Request)
}
