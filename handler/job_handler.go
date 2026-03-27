package handler

import (
	"net/http"

	"github.com/ankush/go-jobs/db"
	"github.com/ankush/go-jobs/models"
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

	//sacve he databse
	db.DB.Create(&job)

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
