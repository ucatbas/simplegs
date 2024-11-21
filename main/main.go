package main

import (
	"context"
	"fmt"
	"math/rand/v2"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/ucatbas/simplegs"
	"github.com/ucatbas/simplegs/models"
)

func main() {
	// Create in-memory storage and initialize the Scheduler
	storage := simplegs.NewInMemoryStorage()
	scheduler := simplegs.NewScheduler(storage, 1*time.Millisecond, 2000)
	maps := map[string]string{}
	const totalTasks = 1000

	// Run the improved performance test
	go scheduler.Start(20, 100)
	go runPerformanceTest(scheduler, &maps)

	// Set up channel to wait for an interrupt or termination signal
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	fmt.Println("Scheduler is running. Press Ctrl+C to stop.")
	<-stop // Block until an interrupt signal is received

	fmt.Println("Shutting down scheduler...")
	verifyTaskResults(totalTasks, storage, maps)

	scheduler.Stop() // Gracefully stop the scheduler

	fmt.Println("Scheduler stopped. Exiting.")
}

func runPerformanceTest(scheduler *simplegs.Scheduler, maps_pointer *map[string]string) {
	fmt.Println("Starting Performance Test")
	maps := *maps_pointer
	const totalTasks = 1000
	cancelProbability := 0.1  // 10% chance to cancel a job
	triggerProbability := 0.1 // 10% chance to manually trigger a job

	cancelCount := 0
	triggerCount := 0

	var mu sync.Mutex // To safely update cancelCount and triggerCount
	// Step 1: Create and post tasks concurrently
	for i := 0; i < totalTasks; i++ {

		taskID := fmt.Sprintf("task-%d", i)
		task := models.NewTask(taskID, func() *models.TaskResponse {
			time.Sleep(time.Millisecond * 5) // Simulate variable processing time
			return &models.TaskResponse{Data: fmt.Sprintf("Task %s completed", taskID), Error: nil}
		})
		scheduledTime := time.Now().Add(time.Second * 6) // Introduce random scheduling
		request := models.NewRequest(context.Background(), task, scheduledTime)
		maps[taskID] = request.ID
		if err := scheduler.PostJob(request); err != nil {
			fmt.Printf("Failed to post job %s: %v", taskID, err)
		}
	}
	//Step 2: Randomly cancel or trigger tasks
	for i := 0; i < totalTasks; i++ {
		if rand.Float64() < cancelProbability {
			jobId := maps[fmt.Sprintf("task-%d", i)]
			if err := scheduler.CancelJob(jobId); err == nil {
				mu.Lock()
				cancelCount++
				mu.Unlock()
			} else {
				fmt.Println(err)
			}
		} else if rand.Float64() < triggerProbability {
			jobId := maps[fmt.Sprintf("task-%d", i)]
			if err := scheduler.TriggerJob(jobId); err == nil {
				mu.Lock()
				triggerCount++
				mu.Unlock()
			}
		}
	}

	// Step 3: Wait for test completion and verify results
	fmt.Println("Test running, waiting for tasks to complete...")
}

func verifyTaskResults(totalTasks int, storage simplegs.Storage, maps map[string]string) {
	go func() {

		fmt.Println("Verifying task results from storage...")
		completedCount := 0
		pendingCount := 0
		cancelledCount := 0
		for i := 0; i < totalTasks; i++ {
			taskID := fmt.Sprintf("task-%d", i)
			job, err := storage.GetJobByID(maps[taskID])
			if err != nil {
				fmt.Printf("Failed to fetch status for task %s: %v", taskID, err)
				continue
			}

			if job.Status == "completed" {
				completedCount++
			} else if job.Status == "pending" {
				pendingCount++
			} else if job.Status == "canceled" {
				cancelledCount++
			}

		}
		fmt.Printf("Total Tasks: %d, Completed: %d, Canceled: %d, Pending: %d", totalTasks, completedCount, cancelledCount, pendingCount)

	}()

}
