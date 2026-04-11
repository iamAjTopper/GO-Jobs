package main

import (
	"log"

	"github.com/ankush/go-jobs/db"
	"github.com/ankush/go-jobs/handler"
	"github.com/ankush/go-jobs/models"
	"github.com/ankush/go-jobs/worker"
	"github.com/gin-gonic/gin"
)

func main() {
	db.Connect()
	db.ConnectRedis()

	db.RDB.XGroupCreateMkStream(db.Ctx, "jobs_stream_free", "workers", "0")
	db.RDB.XGroupCreateMkStream(db.Ctx, "jobs_stream_premium", "workers", "0")

	err := db.RDB.XGroupCreateMkStream(db.Ctx, "jobs_stream", "workers", "0").Err()
	if err != nil {
		log.Println("Consumer group may already exists", err)
	}

	db.DB.AutoMigrate(&models.Job{})

	r := gin.Default()
	//routes
	r.POST("/jobs", handler.CreateJob)
	r.GET("/jobs", handler.GetJobs)
	r.GET("/jobs/:id", handler.GetJobsById)

	//start worker her
	go worker.StartWorker("worker-free", "jobs_stream_free")
	go worker.StartWorker("worker-premium", "jobs_stream_premium")

	r.GET("/", func(c *gin.Context) {
		c.JSON(200, gin.H{"message": "Server running"})
	})
	r.Run(":8080")
}
