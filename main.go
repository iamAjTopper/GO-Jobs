package main

import (
	"github.com/ankush/go-jobs/db"
	"github.com/ankush/go-jobs/handler"
	"github.com/ankush/go-jobs/models"
	"github.com/ankush/go-jobs/worker"
	"github.com/gin-gonic/gin"
)

func main() {
	db.Connect()

	db.DB.AutoMigrate(&models.Job{})

	r := gin.Default()
	//routes
	r.POST("/jobs", handler.CreateJob)
	r.GET("/jobs", handler.GetJobs)
	r.GET("/jobs/:id", handler.GetJobsById)

	//start worker her
	go worker.StartWorker("worker-1")
	go worker.StartWorker("worker-2")

	r.GET("/", func(c *gin.Context) {
		c.JSON(200, gin.H{"message": "Server running"})
	})
	r.Run(":8080")
}
