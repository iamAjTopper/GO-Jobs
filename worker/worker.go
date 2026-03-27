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
	//stimulaing work
	time.Sleep(2 * time.Second)

	//simulating failure
	if job.ID%3 == 0 {
		log.Printf("[%s] Job %d failed \n", workerName, job.ID)
		//retring fsiled work
		db.DB.Model(&job).Updates(map[string]interface{}{
			"status":  "pending",
			"retries": job.Retries + 1,
		})
		return
	}
	log.Printf("[%s] Job %d completed\n", workerName, job.ID)

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
