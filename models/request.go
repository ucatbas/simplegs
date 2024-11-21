package models

import (
	"context"
	"errors"
	"time"

	"github.com/google/uuid"
)

type Request struct {
	ID               string             `json:"id"`
	Task             *Task              `json:"task"`
	ScheduledTime    time.Time          `json:"scheduled_time"`
	CompletedChannel chan *TaskResponse `json:"-"`
	Ctx              context.Context    `json:"-"`
	CreatedAt        time.Time          `json:"created_at"`
	Status           string             `json:"status"`
}

// NewRequest creates a new job request with a single task.
func NewRequest(ctx context.Context, task *Task, scheduledTime time.Time) *Request {
	// Generate unique ID if not provided
	requestID := uuid.NewString()

	return &Request{
		ID:               requestID,
		Ctx:              ctx,
		Task:             task,
		CompletedChannel: make(chan *TaskResponse, 1),
		CreatedAt:        time.Now(),
		ScheduledTime:    scheduledTime,
		Status:           "pending", // Default status
	}
}

// Validate checks if the request is properly structured and all tasks are valid.
func (r *Request) Validate() error {
	if r.CompletedChannel == nil {
		return errors.New("CompletedChannel is nil")
	}
	if r.Task == nil {
		return errors.New("task is empty")
	}

	if err := r.Task.Validate(); err != nil {
		return err
	}
	return nil
}

// MarkComplete sets the status of the request to "completed" and notifies via the channel.
func (r *Request) MarkComplete(resp *TaskResponse) {
	r.Status = "completed"
	if r.CompletedChannel != nil {
		r.CompletedChannel <- resp
	}
}

// MarkCanceled updates the job's status to "canceled"
func (r *Request) MarkCanceled() {
	r.Status = "canceled"
}

// IsCanceled checks if the job is canceled
func (r *Request) IsCanceled() bool {
	return r.Status == "canceled"
}
