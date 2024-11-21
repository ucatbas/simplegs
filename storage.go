package simplegs

import (
	"github.com/ucatbas/simplegs/models"
)

type Storage interface {
	Save(job *models.Request) error
	GetJobByID(jobID string) (*models.Request, error)
	LoadAll() ([]*models.Request, error)
	Delete(jobID string) error
	GetScheduledJobs() ([]*models.Request, error)
}
