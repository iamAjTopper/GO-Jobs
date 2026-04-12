package worker

import (
	"fmt"
	"log"
	"strconv"
	"sync"
	"time"

	"github.com/ankush/go-jobs/db"
	"github.com/ankush/go-jobs/models"
	"github.com/redis/go-redis/v9"
)

var Wg sync.WaitGroup
var Shutdown = false

// each worker will have its own pool (so free & premium don't block each other)
func StartWorker(name string, stream string) {

	//limit concurrency → max 3 jobs at once per worker
	var workerPool = make(chan struct{}, 3)

	for !Shutdown {

		// 👇 ONLY one worker handles reclaim (avoid duplicate reclaim chaos)
		if name == "worker-free" && !Shutdown {
			reclaimStuckJobs(name, stream)
		}

		//if shutdown triggered → exit loop immediately
		if Shutdown {
			break
		}

		//waiting for the job from redis
		//XREADGROUP will block shortly so shutdown is responsive
		streams, err := db.RDB.XReadGroup(db.Ctx, &redis.XReadGroupArgs{
			Group:    "workers",              //all workers share this group
			Consumer: name,                   //worker identity (worker-free / worker-premium)
			Streams:  []string{stream, ">"},  //IMPORTANT → use dynamic stream (free/premium)
			Count:    1,                      //grab 1 job at a time
			Block:    500 * time.Millisecond, //short wait → faster shutdown response
		}).Result()

		//handle redis errors properly
		if err != nil {
			if err == redis.Nil {
				continue //no new job → normal behavior
			}
			log.Println("Redis read error:", err)
			continue
		}

		//safety check (sometimes redis returns empty)
		if len(streams) == 0 || len(streams[0].Messages) == 0 {
			continue
		}

		//get the first message from stream
		msg := streams[0].Messages[0]

		//streams store everything as interface{} → convert safely
		jobIDStr := fmt.Sprintf("%v", msg.Values["job_id"])
		jobID, err := strconv.Atoi(jobIDStr)
		if err != nil {
			log.Println("Invalid job_id:", err)
			continue
		}

		//fetch full job data from postgres
		var job models.Job
		db.DB.First(&job, jobID)

		//do not start new jobs during shutdown
		if Shutdown {
			break
		}

		//acquire slot → if 3 jobs already running, this will wait
		workerPool <- struct{}{}

		//IMPORTANT: copy values to avoid goroutine issues
		jobCopy := job
		msgID := msg.ID

		//run job in goroutine (parallel execution)
		Wg.Add(1)

		go func() {
			defer Wg.Done()
			//release slot when done (VERY IMPORTANT)
			defer func() { <-workerPool }()

			//now log when actually processing starts (not before)
			log.Printf("[%s] Processing job ID: %d\n", name, jobCopy.ID)

			//process job
			success := processJob(jobCopy, name)

			//ACK only if job completed successfully
			//this removes it from PEL (important for reliability)
			if success {
				err := db.RDB.XAck(db.Ctx, stream, "workers", msgID).Err()
				if err != nil {
					log.Println("Failed to ACK:", err)
				}
			}
		}()
	}
}

func processJob(job models.Job, workerName string) bool {
	time.Sleep(8 * time.Second)

	if job.Processed {
		log.Printf("[%s] Job %d already processed, skipping\n", workerName, job.ID)
		return true
	}

	switch job.Type {

	case "email":
		log.Printf("[%s] Sending email for job %d\n", workerName, job.ID)

	case "report":
		log.Printf("[%s] Generating report for job %d\n", workerName, job.ID)

	case "fail":
		if job.Retries >= 3 {
			log.Printf("[%s] Job %d permanently failed\n", workerName, job.ID)
			db.DB.Model(&job).Updates(map[string]interface{}{
				"status":    "failed",
				"processed": true,
			})
			return true // stop retrying → ACK it
		}

		//backoff
		delay := time.Duration((job.Retries+1)*2) * time.Second
		log.Printf("[%s] Job %d failed (retry %d) -> waiting %v\n", workerName, job.ID, job.Retries+1, delay)
		time.Sleep(delay)

		db.DB.Model(&job).Updates(map[string]interface{}{
			"status":  "pending",
			"retries": job.Retries + 1,
		})

		return false // do NOT ACK → stays in PEL

	default:
		log.Printf("[%s] Unknown job type for job %d\n", workerName, job.ID)
		return true
	}

	db.DB.Model(&job).Updates(map[string]interface{}{
		"status":    "done",
		"processed": true,
	})
	return true
}

var lastId = "0"

func reclaimStuckJobs(name string, stream string) {
	res, nextID, err := db.RDB.XAutoClaim(db.Ctx, &redis.XAutoClaimArgs{
		Stream:   stream,
		Group:    "workers",
		Consumer: name,             //the worker whihc is douing the reclaiming
		MinIdle:  10 * time.Second, //onlty grab jobs that have been stuck
		Start:    lastId,           //cursor for scanning
		Count:    5,                //grab up to 5 stuck jobs
	}).Result()

	if err != nil {
		log.Println("XAUTOCLAIM error", err)
		return
	}

	//update cursor
	lastId = nextID

	if len(res) == 0 {
		return
	}

	log.Println("Reclaimed jobs found...")

	for _, msg := range res {

		//if shutting down → stop reclaim processing
		if Shutdown {
			return
		}

		//identify jobs from the redis message
		jobIDStr := fmt.Sprintf("%v", msg.Values["job_id"])
		jobId, _ := strconv.Atoi(jobIDStr)

		var job models.Job
		db.DB.First(&job, jobId)

		//skip already completed jobs (VERY IMPORTANT)
		if job.Status == "done" || job.Status == "failed" {
			continue
		}

		//skip already processed jobs (extra safety)
		if job.Processed {
			continue
		}

		//run reclaimed job in goroutine (same as normal flow)
		Wg.Add(1)

		jobCopy := job
		msgID := msg.ID

		go func() {
			defer Wg.Done()

			log.Printf("[%s] Reclaimed job ID: %d\n", name, jobCopy.ID)

			success := processJob(jobCopy, name)

			if success {
				err := db.RDB.XAck(db.Ctx, stream, "workers", msgID).Err()
				if err != nil {
					log.Println("Failed to ACK:", err)
				}
			}
		}()
	}

	//small delay to avoid aggressive reclaim loops
	time.Sleep(2 * time.Second)
}
