package handler

import (
	"log"
	"net/http"

	"github.com/ankush/go-jobs/shared/db"
	"github.com/ankush/go-jobs/shared/models"
	"github.com/gin-gonic/gin"
	"github.com/redis/go-redis/v9"
)

func CreateJob(c *gin.Context) {
	//empy variable
	var job models.Job

	//reading json from request and binding
	if err := c.ShouldBindJSON(&job); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
		return
	}
	//saving some of he values from jobs model o default
	job.Status = "pending"
	job.Retries = 0

	//save he databse
	db.DB.Create(&job)

	//pushing job id to reddis queue

	stream := "jobs_stream_free"

	if job.Priority == "premium" {
		stream = "jobs_stream_premium"
	}
	_, err := db.RDB.XAdd(db.Ctx, &redis.XAddArgs{
		Stream: stream,
		Values: map[string]interface{}{
			"job_id": job.ID,
		},
	}).Result()

	if err != nil {
		log.Println("Failed to push job to Redis stream:", err)
	}

	//send back 200
	c.JSON(http.StatusOK, job)

}

func GetJobs(c *gin.Context) {
	var jobs []models.Job
	db.DB.Find(&jobs)
	c.JSON(200, jobs)
}

func GetJobsById(c *gin.Context) {
	id := c.Param("id")

	var job models.Job
	result := db.DB.First(&job, id)

	if result.Error != nil {
		c.JSON(404, gin.H{"error": "Job not found"})
		return
	}
	c.JSON(200, job)
}
