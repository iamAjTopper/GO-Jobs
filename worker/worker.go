package worker

import (
	"log"
	"time"

	"github.com/ankush/go-jobs/db"

	"github.com/ankush/go-jobs/models"
)

func StartWorker(name string) {
	//infinite loop to keep the worker running
	for {
		recoverStuckJobs()

		job, err := fetchJob()

		if err != nil {
			log.Println("Error fetching jobs", err)
			continue
		}
		//if no job availabe telling the wroker to sleep for 2sec
		if job == nil {
			time.Sleep(2 * time.Second)
			continue
		}
		log.Printf("[%s] Processing job ID: %d\n", name, job.ID)
		processJob(*job, name)
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

		return

	default:
		log.Printf("[%s] Unknown job type for job %d\n", workerName, job.ID)
	}

	db.DB.Model(&job).Update("status", "done")
}

func fetchJob() (*models.Job, error) {
	//new job var
	var job models.Job

	//starting the transaction
	tx := db.DB.Begin()

	//the p[ower query
	err := tx.Raw(`SELECT * FROM jobs
	WHERE status = 'pending'
	ORDER BY id
	FOR UPDATE SKIP LOCKED
	LIMIT 1
	`).Scan(&job).Error

	//safety check
	if err != nil {
		tx.Rollback()
		return nil, err
	}

	//no job found
	if job.ID == 0 {
		tx.Rollback()
		return nil, nil
	}
	//marking as processing inside same transaction
	tx.Model(&job).Update("status", "processing")

	//commitng he chanegs
	tx.Commit()

	return &job, nil
}

func recoverStuckJobs() {
	db.DB.Exec(`
	UPDATE jobs
	SET status = 'pending'
	WHERE status = 'processing'
	AND updated_at < NOW() - INTERVAL '30 seconds'
	`)
}
