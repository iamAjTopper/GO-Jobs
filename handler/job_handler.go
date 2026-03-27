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
