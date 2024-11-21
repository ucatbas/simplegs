package models

import (
	"encoding/json"
	"errors"
	"time"
)

// Response structure for a task execution.
type TaskResponse struct {
	Data  interface{}
	Error error
}

// Callback defines the processing logic of a task.
type Callback func() *TaskResponse

// Represents a task to be executed.
type Task struct {
	ID         string   // Unique ID for the task
	Callback   Callback `json:"-"` // Exclude this field from marshaling
	Timeout    time.Duration
	RetryCount int
}

// MarshalJSON is a custom marshaler for Task that excludes the Callback function.
func (t *Task) MarshalJSON() ([]byte, error) {
	type Alias Task // Create an alias to avoid recursion
	return json.Marshal(&struct {
		Callback interface{} `json:"callback,omitempty"` // Placeholder for Callback
		*Alias
	}{
		Callback: nil, // Explicitly set Callback to nil or remove
		Alias:    (*Alias)(t),
	})
}

// UnmarshalJSON is a custom unmarshaler for Task.
func (t *Task) UnmarshalJSON(data []byte) error {
	type Alias Task
	aux := &struct {
		Callback interface{} `json:"callback"`
		*Alias
	}{
		Alias: (*Alias)(t),
	}
	if err := json.Unmarshal(data, aux); err != nil {
		return err
	}
	// Optional: Reassign Callback function if needed after unmarshaling.
	t.Callback = nil // Leave as nil or assign a default function.
	return nil
}

// Create a new task with an ID and callback.
func NewTask(id string, callback Callback) *Task {
	return &Task{ID: id, Callback: callback}
}

// Set timeout in milliseconds for a task.
func (t *Task) WithMilliSecondTimeout(d int) *Task {
	t.Timeout = time.Duration(d) * time.Millisecond
	return t
}

// Set timeout in seconds for a task.
func (t *Task) WithSecondTimeout(d int) *Task {
	t.Timeout = time.Duration(d) * time.Second
	return t
}

// Set retry count for a task in case of failure.
func (t *Task) WithRetry(count int) *Task {
	t.RetryCount = count
	return t
}

// Validate checks the Task's properties for validity.
func (t *Task) Validate() error {
	// Ensure the task ID is non-empty.
	if t.ID == "" {
		return errors.New("task ID is required")
	}

	// Ensure the Callback function is set.
	if t.Callback == nil {
		return errors.New("callback function is required")
	}

	// Ensure the timeout is non-negative.
	if t.Timeout < 0 {
		return errors.New("timeout must be non-negative")
	}

	// Ensure the retry count is non-negative.
	if t.RetryCount < 0 {
		return errors.New("retry count must be non-negative")
	}

	return nil
}
