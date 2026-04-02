package worker

import (
	"log"
	"time"

	"github.com/ankush/go-jobs/db"

	"github.com/ankush/go-jobs/models"
)

func StartWorker(name string) {
	for {
		//waiting for the jiob fro9m redis
		// BRPOP will "block" (pause the code) until a job ID is available
		// The '0' means wait forever until something arrives
		result, err := db.RDB.BRPop(db.Ctx, 0, "jobs_queue").Result()
		if err != nil {
			log.Println("Redis error:", err)
			continue
		}
		//1 will be the real job that we opushed
		jobID := result[1]

		//getting the full data fro he postgres now
		var job models.Job
		db.DB.First(&job, jobID)

		log.Printf("[%s] Processing job ID: %d\n", name, job.ID)

		processJob(job, name)
	}
}

func processJob(job models.Job, workerName string) {
	time.Sleep(2 * time.Second)

	switch job.Type {

	case "email":
		log.Printf("[%s] Sending email for job %d\n", workerName, job.ID)

	case "report":
		log.Printf("[%s] Generating report for job %d\n", workerName, job.ID)

	case "fail":
		if job.Retries >= 3 {
			log.Printf("[%s] Job %d permanently failed\n", workerName, job.ID)
			db.DB.Model(&job).Update("status", "failed")
			return
		}

		log.Printf("[%s] Job %d failed (retry %d)\n", workerName, job.ID, job.Retries+1)

		db.DB.Model(&job).Updates(map[string]interface{}{
			"status":  "pending",
			"retries": job.Retries + 1,
		})

		//requeue in redis
		err := db.RDB.LPush(db.Ctx, "jobs_queue", job.ID).Err()
		if err != nil {
			log.Println("Failed to requeue job:", err)
		}
		return

	default:
		log.Printf("[%s] Unknown job type for job %d\n", workerName, job.ID)
	}

	db.DB.Model(&job).Update("status", "done")
}
