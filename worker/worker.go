package worker

import (
	"log"
	"strconv"
	"time"

	"github.com/ankush/go-jobs/db"

	"github.com/redis/go-redis/v9"

	"github.com/ankush/go-jobs/models"
)

func StartWorker(name string) {
	for {
		//waiting for the jiob fro9m redis
		// BRPOP will "block" (pause the code) until a job ID is available
		// The '0' means wait forever until something arrives
		streams, err := db.RDB.XReadGroup(db.Ctx, &redis.XReadGroupArgs{
			Group:    "workers",                    //the team name, all worker in his group sdhare the load
			Consumer: name,                         //the employee name like worker 1
			Streams:  []string{"jobs_stream", ">"}, // the > means Give me ONLY new messages that no one else has seen
			Count:    1,                            //just grab one job at a time
			Block:    0,                            //eait forever until job returns
		}).Result()

		if err != nil {
			log.Println("Redis read errro:", err)
			continue
		}

		//safety check
		if len(streams) == 0 || len(streams[0].Messages) == 0 {
			continue
		}

		////digging into the resul to get the frist message
		msg := streams[0].Messages[0]

		//streams store everything as strings/interface so we convert job_id back to inetger
		jobIDStr := msg.Values["job_id"].(string)
		jobID, err := strconv.Atoi(jobIDStr)

		if err != nil {
			log.Println("Invalid job_id", err)
			continue
		}

		//Go get the actual data from our postgres
		var job models.Job
		db.DB.First(&job, jobID)

		log.Printf("[%s] Processing job ID: %d\n", name, job.ID)

		//do the work
		success := processJob(job, name)

		//ACK only if success
		if success {
			err := db.RDB.XAck(db.Ctx, "jobs_stream", "worker", msg.ID).Err()
			if err != nil {
				log.Println("Failed to ACK:", err)
			}
		}

	}
}

func processJob(job models.Job, workerName string) bool {
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
			return true // stop retrying → ACK it
		}

		log.Printf("[%s] Job %d failed (retry %d)\n", workerName, job.ID, job.Retries+1)

		db.DB.Model(&job).Updates(map[string]interface{}{
			"status":  "pending",
			"retries": job.Retries + 1,
		})

		return false // do NOT ACK → stays in PEL

	default:
		log.Printf("[%s] Unknown job type for job %d\n", workerName, job.ID)
		return true
	}

	db.DB.Model(&job).Update("status", "done")
	return true
}
