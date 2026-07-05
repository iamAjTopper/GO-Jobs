package handler

import (
	"net/http"

	"github.com/ankush/go-jobs/shared/db"
	"github.com/ankush/go-jobs/shared/models"
	"github.com/gin-gonic/gin"
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

	//START THE TRANSCATION
	tx := db.DB.Begin()

	//saver ther databse
	if err := tx.Create(&job).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"error": "failed to crteate job"})
		return
	}

	//decidinfg which stram based ion prioroty
	stream := "jobs_stream_free"
	if job.Priority == "premium" {
		stream = "jobs_stream_premium"
	}

	//insert into outbox tbale instyead of pusghimg directly into redis
	outbox := models.Outbox{
		JobID:  job.ID,
		Status: "pending",
		Stream: stream, //worker know where tio push
	}

	if err := tx.Create(&outbox).Error; err != nil {
		tx.Rollback()
		c.JSON(http.StatusInternalServerError, gin.H{"error": "faield to create outbox"})
		return
	}

	//commit transaction
	tx.Commit()

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
