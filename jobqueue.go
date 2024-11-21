package simplegs

import (
	"github.com/ucatbas/simplegs/models"
)

type JobQueue []*models.Request

func (jq JobQueue) Len() int { return len(jq) }

func (jq JobQueue) Less(i, j int) bool {
	// Jobs are sorted by their scheduled execution time (earliest first)
	return jq[i].ScheduledTime.Before(jq[j].ScheduledTime)
}

func (jq JobQueue) Swap(i, j int) {
	jq[i], jq[j] = jq[j], jq[i]
}

func (jq *JobQueue) Push(x interface{}) {
	*jq = append(*jq, x.(*models.Request))
}

func (jq *JobQueue) Pop() interface{} {
	old := *jq
	n := len(old)
	item := old[n-1]
	*jq = old[0 : n-1]
	return item
}
